package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/dlq"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/ingress"
	"github.com/evalops/ensemble-tap/internal/normalize"
	"github.com/evalops/ensemble-tap/internal/poller"
	pollproviders "github.com/evalops/ensemble-tap/internal/poller/providers"
	"github.com/evalops/ensemble-tap/internal/publish"
	"github.com/evalops/ensemble-tap/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

type readiness interface {
	Ready() error
}

const (
	defaultReplayDLQLimit = 100
	maxReplayDLQLimit     = 2000

	adminEndpointReplayDLQ    = "replay_dlq"
	adminEndpointPollerStatus = "poller_status"
	adminOutcomeSuccess       = "success"
	adminOutcomeUnauthorized  = "unauthorized"
	adminOutcomeRateLimited   = "rate_limited"
	adminOutcomeBadRequest    = "bad_request"
	adminOutcomeInternalError = "error"
)

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate runtime config: %w", err)
	}

	metrics := health.NewMetrics()
	publisher, err := publish.NewNATSPublisher(ctx, cfg.NATS, metrics)
	if err != nil {
		return fmt.Errorf("initialize nats publisher: %w", err)
	}
	defer publisher.Close()

	clickhouseSink, err := publish.NewClickHouseSink(ctx, cfg.ClickHouse, cfg.NATS, publisher.JetStream(), metrics)
	if err != nil {
		return fmt.Errorf("initialize clickhouse sink: %w", err)
	}
	if clickhouseSink != nil {
		if err := clickhouseSink.Start(ctx); err != nil {
			return fmt.Errorf("start clickhouse sink: %w", err)
		}
		defer clickhouseSink.Close()
	}

	checkpointStore, snapshotStore, storesCloser, err := openPollStores(cfg.State)
	if err != nil {
		return err
	}
	if storesCloser != nil {
		defer storesCloser.Close()
	}

	dlqPublisher, err := dlq.NewPublisher(ctx, cfg.NATS, publisher.JetStream())
	if err != nil {
		return fmt.Errorf("initialize dlq publisher: %w", err)
	}

	pollerStatuses := newPollerStatusRegistry()
	startConfiguredPollers(ctx, cfg, publisher, dlqPublisher, logger, checkpointStore, snapshotStore, pollerStatuses)

	ingressServer := ingress.NewServer(cfg, publisher, metrics, logger)
	ingressServer.SetDLQRecorder(dlqPublisher)
	mux := http.NewServeMux()
	mux.Handle("/", ingressServer.Routes())
	mux.Handle("GET /livez", health.LivenessHandler())
	mux.Handle("GET /readyz", health.ReadinessHandler(func() error {
		if rd, ok := any(publisher).(readiness); ok {
			return rd.Ready()
		}
		return nil
	}))
	mux.Handle("GET /metrics", promhttp.Handler())
	if strings.TrimSpace(cfg.Server.AdminToken) != "" {
		adminToken := strings.TrimSpace(cfg.Server.AdminToken)
		adminTokenSecondary := strings.TrimSpace(cfg.Server.AdminTokenSecondary)
		adminReplayMaxLimit := cfg.Server.AdminReplayMaxLimit
		if adminReplayMaxLimit <= 0 {
			adminReplayMaxLimit = maxReplayDLQLimit
		}
		adminRateLimitPerSec := cfg.Server.AdminRateLimitPerSec
		if adminRateLimitPerSec <= 0 {
			adminRateLimitPerSec = 1
		}
		adminRateLimitBurst := cfg.Server.AdminRateLimitBurst
		if adminRateLimitBurst <= 0 {
			adminRateLimitBurst = 1
		}
		retryAfterSeconds := adminRetryAfterSeconds(adminRateLimitPerSec)
		adminLimiters := map[string]*rate.Limiter{
			adminEndpointReplayDLQ:    rate.NewLimiter(rate.Limit(adminRateLimitPerSec), adminRateLimitBurst),
			adminEndpointPollerStatus: rate.NewLimiter(rate.Limit(adminRateLimitPerSec), adminRateLimitBurst),
		}
		observeAdmin := func(endpoint, outcome string, startedAt time.Time) {
			metrics.AdminRequestsTotal.WithLabelValues(endpoint, outcome).Inc()
			metrics.AdminRequestDurationSeconds.WithLabelValues(endpoint, outcome).Observe(time.Since(startedAt).Seconds())
		}
		requireAdminToken := func(endpoint string, w http.ResponseWriter, r *http.Request) (string, string, bool) {
			reqID := requestID(r)
			userAgent := strings.TrimSpace(r.UserAgent())
			requestIP := requesterIP(r)
			w.Header().Set("X-Request-ID", reqID)
			startedAt := time.Now()
			if limiter, ok := adminLimiters[endpoint]; ok && !limiter.Allow() {
				observeAdmin(endpoint, adminOutcomeRateLimited, startedAt)
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
				logger.Warn("admin request rate limited",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", endpoint,
					"request_id", reqID,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"retry_after_seconds", retryAfterSeconds,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusTooManyRequests, reqID, "rate limit exceeded")
				return reqID, "", false
			}
			authorized, tokenSlot := authorizeAdminToken(strings.TrimSpace(r.Header.Get("X-Admin-Token")), adminToken, adminTokenSecondary)
			if !authorized {
				observeAdmin(endpoint, adminOutcomeUnauthorized, startedAt)
				logger.Warn("admin request unauthorized",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", endpoint,
					"request_id", reqID,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusUnauthorized, reqID, "unauthorized")
				return reqID, "", false
			}
			return reqID, tokenSlot, true
		}

		mux.HandleFunc("POST /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
			reqID, tokenSlot, ok := requireAdminToken(adminEndpointReplayDLQ, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			userAgent := strings.TrimSpace(r.UserAgent())
			requestIP := requesterIP(r)
			requestedLimit, limit, capped, err := parseReplayDLQLimit(r.URL.Query().Get("limit"), adminReplayMaxLimit)
			if err != nil {
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", reqID,
					"token_slot", tokenSlot,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"requested_limit_raw", strings.TrimSpace(r.URL.Query().Get("limit")),
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, reqID, err.Error())
				return
			}
			replayed, err := dlqPublisher.Replay(r.Context(), limit, func(ctx context.Context, subject string, payload []byte, dedupID string) error {
				return publisher.PublishRaw(ctx, subject, payload, dedupID)
			})
			if err != nil {
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeInternalError, startedAt)
				logger.Error("admin replay dlq failed",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", reqID,
					"token_slot", tokenSlot,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"requested_limit", requestedLimit,
					"effective_limit", limit,
					"capped", capped,
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusInternalServerError, reqID, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			response := map[string]any{
				"request_id":      reqID,
				"replayed":        replayed,
				"effective_limit": limit,
				"max_limit":       adminReplayMaxLimit,
				"capped":          capped,
			}
			if requestedLimit > 0 {
				response["requested_limit"] = requestedLimit
			}
			_ = json.NewEncoder(w).Encode(response)
			observeAdmin(adminEndpointReplayDLQ, adminOutcomeSuccess, startedAt)
			logger.Info("admin replay dlq completed",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayDLQ,
				"request_id", reqID,
				"token_slot", tokenSlot,
				"requester_ip", requestIP,
				"user_agent", userAgent,
				"requested_limit", requestedLimit,
				"effective_limit", limit,
				"capped", capped,
				"replayed_count", replayed,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		})
		mux.HandleFunc("GET /admin/poller-status", func(w http.ResponseWriter, r *http.Request) {
			reqID, tokenSlot, ok := requireAdminToken(adminEndpointPollerStatus, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			userAgent := strings.TrimSpace(r.UserAgent())
			requestIP := requesterIP(r)
			providerFilter := strings.TrimSpace(r.URL.Query().Get("provider"))
			tenantFilter := strings.TrimSpace(r.URL.Query().Get("tenant"))
			statuses := pollerStatuses.SnapshotFiltered(providerFilter, tenantFilter)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"generated_at": time.Now().UTC(),
				"request_id":   reqID,
				"provider":     providerFilter,
				"tenant":       tenantFilter,
				"count":        len(statuses),
				"pollers":      statuses,
			})
			observeAdmin(adminEndpointPollerStatus, adminOutcomeSuccess, startedAt)
			logger.Info("admin poller status fetched",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointPollerStatus,
				"request_id", reqID,
				"token_slot", tokenSlot,
				"requester_ip", requestIP,
				"user_agent", userAgent,
				"provider_filter", providerFilter,
				"tenant_filter", tenantFilter,
				"poller_count", len(statuses),
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		})
	}

	httpServer := ingressServer.HTTPServer(mux)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("ensemble-tap started", "addr", httpServer.Addr, "base_path", cfg.Server.BasePath)
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server failed: %w", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := ingressServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	publisher.WaitForClosed(3 * time.Second)
	return nil
}

type closer interface {
	Close() error
}

func secureTokenEqual(actual, expected string) bool {
	actual = strings.TrimSpace(actual)
	expected = strings.TrimSpace(expected)
	if actual == "" || expected == "" {
		return false
	}
	actualHash := sha256.Sum256([]byte(actual))
	expectedHash := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(actualHash[:], expectedHash[:]) == 1
}

func parseReplayDLQLimit(raw string, maxLimit int) (requested int, effective int, capped bool, err error) {
	if maxLimit <= 0 {
		maxLimit = maxReplayDLQLimit
	}
	raw = strings.TrimSpace(raw)
	effective = defaultReplayDLQLimit
	if raw == "" {
		if effective > maxLimit {
			effective = maxLimit
			capped = true
		}
		return 0, effective, capped, nil
	}
	requested, err = strconv.Atoi(raw)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid limit %q: must be a positive integer", raw)
	}
	if requested <= 0 {
		return 0, 0, false, fmt.Errorf("invalid limit %q: must be greater than 0", raw)
	}
	effective = requested
	if effective > maxLimit {
		effective = maxLimit
		capped = true
	}
	return requested, effective, capped, nil
}

func adminRetryAfterSeconds(limitPerSec float64) int {
	if limitPerSec <= 0 {
		return 1
	}
	retry := int(math.Ceil(1 / limitPerSec))
	if retry < 1 {
		return 1
	}
	return retry
}

func authorizeAdminToken(actual, primary, secondary string) (authorized bool, tokenSlot string) {
	primaryMatch := secureTokenEqual(actual, primary)
	secondaryMatch := false
	if strings.TrimSpace(secondary) != "" {
		secondaryMatch = secureTokenEqual(actual, secondary)
	}
	if primaryMatch {
		return true, "primary"
	}
	if secondaryMatch {
		return true, "secondary"
	}
	return false, ""
}

func requesterIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	if raw := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); raw != "" {
		first := strings.TrimSpace(strings.Split(raw, ",")[0])
		if first != "" {
			return first
		}
	}
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	remoteAddr := strings.TrimSpace(r.RemoteAddr)
	if remoteAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil && host != "" {
		return host
	}
	return remoteAddr
}

func requestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id := strings.TrimSpace(r.Header.Get("X-Request-ID")); id != "" {
		return id
	}
	if id := strings.TrimSpace(r.Header.Get("X-Correlation-ID")); id != "" {
		return id
	}
	return "admin-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}

func writeAdminError(w http.ResponseWriter, status int, reqID, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"request_id": strings.TrimSpace(reqID),
		"error":      strings.TrimSpace(message),
	})
}

func openPollStores(cfg config.StateConfig) (store.CheckpointStore, store.SnapshotStore, closer, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Backend)) {
	case "", "memory", "inmemory":
		return store.NewInMemoryCheckpointStore(), store.NewInMemorySnapshotStore(), nil, nil
	case "sqlite":
		stateStore, err := store.NewSQLiteStateStore(cfg.SQLitePath)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open sqlite poll state store: %w", err)
		}
		return stateStore.Checkpoints, stateStore.Snapshots, stateStore, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported state backend %q", cfg.Backend)
	}
}

type pollSink struct {
	publisher               cloudEventPublisher
	dlq                     *dlq.Publisher
	subjectPrefix           string
	tenantScopedNATSSubject bool
}

type pollerStatusSnapshot struct {
	Provider            string    `json:"provider"`
	TenantID            string    `json:"tenant_id,omitempty"`
	Interval            string    `json:"interval"`
	RateLimitPerSec     float64   `json:"rate_limit_per_sec"`
	Burst               int       `json:"burst"`
	FailureBudget       int       `json:"failure_budget"`
	CircuitBreak        string    `json:"circuit_break_duration"`
	JitterRatio         float64   `json:"jitter_ratio"`
	LastRunAt           time.Time `json:"last_run_at,omitempty"`
	LastSuccessAt       time.Time `json:"last_success_at,omitempty"`
	LastErrorAt         time.Time `json:"last_error_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	LastCheckpoint      string    `json:"last_checkpoint,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
}

type pollerStatusEntry struct {
	mu       sync.RWMutex
	snapshot pollerStatusSnapshot
}

func newPollerStatusEntry(provider, tenantID string, interval time.Duration, rateLimitPerSec float64, burst int, failureBudget int, circuitBreak time.Duration, jitterRatio float64) *pollerStatusEntry {
	return &pollerStatusEntry{
		snapshot: pollerStatusSnapshot{
			Provider:        provider,
			TenantID:        tenantID,
			Interval:        interval.String(),
			RateLimitPerSec: rateLimitPerSec,
			Burst:           burst,
			FailureBudget:   failureBudget,
			CircuitBreak:    circuitBreak.String(),
			JitterRatio:     jitterRatio,
		},
	}
}

func (e *pollerStatusEntry) markRun(checkpoint string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.snapshot.LastRunAt = time.Now().UTC()
	if strings.TrimSpace(checkpoint) != "" {
		e.snapshot.LastCheckpoint = checkpoint
	}
	e.mu.Unlock()
}

func (e *pollerStatusEntry) markSuccess(checkpoint string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.snapshot.LastSuccessAt = time.Now().UTC()
	e.snapshot.LastError = ""
	e.snapshot.LastErrorAt = time.Time{}
	e.snapshot.ConsecutiveFailures = 0
	if strings.TrimSpace(checkpoint) != "" {
		e.snapshot.LastCheckpoint = checkpoint
	}
	e.mu.Unlock()
}

func (e *pollerStatusEntry) markError(err error, checkpoint string) {
	if e == nil || err == nil {
		return
	}
	e.mu.Lock()
	e.snapshot.LastError = err.Error()
	e.snapshot.LastErrorAt = time.Now().UTC()
	e.snapshot.ConsecutiveFailures++
	if strings.TrimSpace(checkpoint) != "" {
		e.snapshot.LastCheckpoint = checkpoint
	}
	e.mu.Unlock()
}

func (e *pollerStatusEntry) snapshotCopy() pollerStatusSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshot
}

type pollerStatusRegistry struct {
	mu      sync.RWMutex
	entries map[string]*pollerStatusEntry
}

func newPollerStatusRegistry() *pollerStatusRegistry {
	return &pollerStatusRegistry{entries: map[string]*pollerStatusEntry{}}
}

func (r *pollerStatusRegistry) upsert(provider, tenantID string, interval time.Duration, rateLimitPerSec float64, burst int, failureBudget int, circuitBreak time.Duration, jitterRatio float64) *pollerStatusEntry {
	if r == nil {
		return nil
	}
	key := poller.StateKey(provider, tenantID)
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok {
		entry = newPollerStatusEntry(provider, tenantID, interval, rateLimitPerSec, burst, failureBudget, circuitBreak, jitterRatio)
		r.entries[key] = entry
		return entry
	}
	entry.mu.Lock()
	entry.snapshot.Interval = interval.String()
	entry.snapshot.RateLimitPerSec = rateLimitPerSec
	entry.snapshot.Burst = burst
	entry.snapshot.FailureBudget = failureBudget
	entry.snapshot.CircuitBreak = circuitBreak.String()
	entry.snapshot.JitterRatio = jitterRatio
	entry.mu.Unlock()
	return entry
}

func (r *pollerStatusRegistry) Snapshot() []pollerStatusSnapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]pollerStatusSnapshot, 0, len(r.entries))
	for _, entry := range r.entries {
		out = append(out, entry.snapshotCopy())
	}
	r.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Provider == out[j].Provider {
			return out[i].TenantID < out[j].TenantID
		}
		return out[i].Provider < out[j].Provider
	})
	return out
}

func (r *pollerStatusRegistry) SnapshotFiltered(provider, tenantID string) []pollerStatusSnapshot {
	provider = strings.TrimSpace(strings.ToLower(provider))
	tenantID = strings.TrimSpace(tenantID)
	snapshots := r.Snapshot()
	if provider == "" && tenantID == "" {
		return snapshots
	}
	filtered := make([]pollerStatusSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if provider != "" && !strings.EqualFold(snapshot.Provider, provider) {
			continue
		}
		if tenantID != "" && snapshot.TenantID != tenantID {
			continue
		}
		filtered = append(filtered, snapshot)
	}
	return filtered
}

type cloudEventPublisher interface {
	Publish(ctx context.Context, event cloudevents.Event, dedupID string) (string, error)
}

func (s pollSink) Publish(ctx context.Context, evt normalize.NormalizedEvent, dedupID string) error {
	ce, err := normalize.ToCloudEvent(evt)
	if err != nil {
		s.recordDLQ(ctx, "poll_normalize", evt, dedupID, nil, err)
		return err
	}
	_, err = s.publisher.Publish(ctx, ce, dedupID)
	if err != nil {
		payload, _ := json.Marshal(ce)
		s.recordDLQ(ctx, "poll_publish", evt, dedupID, payload, err)
	}
	return err
}

func (s pollSink) recordDLQ(ctx context.Context, stage string, evt normalize.NormalizedEvent, dedupID string, payload []byte, reason error) {
	if s.dlq == nil || reason == nil {
		return
	}
	if payload == nil {
		if ce, err := normalize.ToCloudEvent(evt); err == nil {
			payload, _ = json.Marshal(ce)
		}
	}
	_ = s.dlq.Record(ctx, dlq.Record{
		Stage:    stage,
		Provider: evt.Provider,
		TenantID: evt.TenantID,
		Reason:   reason.Error(),
		OriginalSubject: normalize.BuildSubjectWithTenant(
			s.subjectPrefix,
			evt.TenantID,
			evt.Provider,
			evt.EntityType,
			evt.Action,
			s.tenantScopedNATSSubject,
		),
		OriginalDedupID: dedupID,
		OriginalPayload: payload,
	})
}

func startConfiguredPollers(ctx context.Context, cfg config.Config, publisher cloudEventPublisher, dlqPublisher *dlq.Publisher, logger *slog.Logger, checkpointStore store.CheckpointStore, snapshotStore store.SnapshotStore, statuses *pollerStatusRegistry) {
	if checkpointStore == nil || snapshotStore == nil {
		return
	}
	sink := pollSink{
		publisher:               publisher,
		dlq:                     dlqPublisher,
		subjectPrefix:           cfg.NATS.SubjectPrefix,
		tenantScopedNATSSubject: cfg.NATS.TenantScopedSubjects,
	}

	for providerName, pcfg := range cfg.Providers {
		if !modeContainsPoll(pcfg.Mode) {
			continue
		}
		targets := buildPollTargets(pcfg)
		if len(targets) == 0 {
			logger.Warn("poll mode requested but no poll targets resolved", "provider", providerName)
			continue
		}

		for _, target := range targets {
			fetcher := fetcherForProvider(providerName, target)
			if fetcher == nil {
				logger.Warn("poll mode requested but provider poller not available", "provider", providerName, "tenant", target.TenantID)
				continue
			}

			interval := target.PollInterval
			if interval <= 0 {
				interval = time.Minute
			}
			failureBudget, circuitBreakDuration, jitterRatio := pollResilience(target)
			limiter, limitPerSec, burst := pollLimiter(target)
			statusEntry := statuses.upsert(fetcher.ProviderName(), target.TenantID, interval, limitPerSec, burst, failureBudget, circuitBreakDuration, jitterRatio)
			stateKey := poller.StateKey(fetcher.ProviderName(), target.TenantID)

			p := &poller.Poller{
				Provider:             fetcher.ProviderName(),
				Interval:             interval,
				RateLimiter:          limiter,
				FailureBudget:        failureBudget,
				CircuitBreakDuration: circuitBreakDuration,
				JitterRatio:          jitterRatio,
				Run: func(fetcher poller.Fetcher, tenantID string, statusEntry *pollerStatusEntry, stateKey string) poller.PollFn {
					return func(ctx context.Context) error {
						checkpointBefore, _ := checkpointStore.Get(stateKey)
						statusEntry.markRun(checkpointBefore)

						err := poller.RunCycle(ctx, fetcher, checkpointStore, snapshotStore, sink, tenantID)
						checkpointAfter, _ := checkpointStore.Get(stateKey)
						if err != nil {
							statusEntry.markError(err, checkpointAfter)
							return err
						}
						statusEntry.markSuccess(checkpointAfter)
						return nil
					}
				}(fetcher, target.TenantID, statusEntry, stateKey),
			}
			logger.Info("starting provider poller", "provider", providerName, "tenant", target.TenantID, "interval", interval.String())
			go func(p *poller.Poller) {
				p.Start(ctx)
			}(p)
		}
	}
}

func modeContainsPoll(mode string) bool {
	mode = normalizeMode(mode)
	return strings.Contains(mode, "poll")
}

func normalizeMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	mode = strings.ReplaceAll(mode, " ", "")
	return mode
}

func buildPollTargets(base config.ProviderConfig) []config.ProviderConfig {
	targets := make([]config.ProviderConfig, 0)
	seen := map[string]struct{}{}

	addTarget := func(candidate config.ProviderConfig) {
		tenantID := strings.TrimSpace(candidate.TenantID)
		key := tenantID
		if key == "" {
			key = "__default__"
		}
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, candidate)
	}

	if len(base.Tenants) == 0 || hasBasePollCredentials(base) {
		addTarget(config.ApplyProviderTenant(base, strings.TrimSpace(base.TenantID)))
	}

	if len(base.Tenants) == 0 {
		return targets
	}

	tenantKeys := make([]string, 0, len(base.Tenants))
	for tenantKey := range base.Tenants {
		tenantKeys = append(tenantKeys, tenantKey)
	}
	sort.Strings(tenantKeys)
	for _, tenantKey := range tenantKeys {
		addTarget(config.ApplyProviderTenant(base, tenantKey))
	}
	return targets
}

func hasBasePollCredentials(cfg config.ProviderConfig) bool {
	return strings.TrimSpace(cfg.AccessToken) != "" ||
		strings.TrimSpace(cfg.APIKey) != "" ||
		strings.TrimSpace(cfg.Secret) != "" ||
		strings.TrimSpace(cfg.RefreshToken) != ""
}

func pollLimiter(cfg config.ProviderConfig) (*rate.Limiter, float64, int) {
	limitPerSec := cfg.PollRateLimitPerSec
	if limitPerSec <= 0 {
		limitPerSec = 4.0
	}
	burst := cfg.PollBurst
	if burst <= 0 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(limitPerSec), burst), limitPerSec, burst
}

func pollResilience(cfg config.ProviderConfig) (failureBudget int, circuitBreakDuration time.Duration, jitterRatio float64) {
	failureBudget = cfg.PollFailureBudget
	if failureBudget <= 0 {
		failureBudget = 5
	}

	circuitBreakDuration = cfg.PollCircuitBreak
	if circuitBreakDuration <= 0 {
		circuitBreakDuration = 30 * time.Second
	}

	jitterRatio = cfg.PollJitterRatio
	if jitterRatio < 0 {
		jitterRatio = 0
	}
	if jitterRatio > 0.95 {
		jitterRatio = 0.95
	}
	return failureBudget, circuitBreakDuration, jitterRatio
}

func fetcherForProvider(name string, cfg config.ProviderConfig) poller.Fetcher {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "hubspot":
		token := cfg.AccessToken
		if token == "" {
			token = cfg.APIKey
		}
		return &pollproviders.HubSpotFetcher{
			BaseURL:      cfg.BaseURL,
			Token:        token,
			TokenURL:     cfg.TokenURL,
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RefreshToken: cfg.RefreshToken,
			Scope:        cfg.Scope,
			Objects:      cfg.Objects,
			Limit:        cfg.QueryPerPage,
		}
	case "salesforce":
		token := cfg.AccessToken
		if token == "" {
			token = cfg.Secret
		}
		return &pollproviders.SalesforceFetcher{
			BaseURL:      cfg.BaseURL,
			AccessToken:  token,
			TokenURL:     cfg.TokenURL,
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RefreshToken: cfg.RefreshToken,
			Scope:        cfg.Scope,
			APIVersion:   cfg.APIVersion,
			Objects:      cfg.Objects,
			QueryPerPage: cfg.QueryPerPage,
		}
	case "quickbooks":
		token := cfg.AccessToken
		if token == "" {
			token = cfg.Secret
		}
		return &pollproviders.QuickBooksFetcher{
			BaseURL:      cfg.BaseURL,
			AccessToken:  token,
			TokenURL:     cfg.TokenURL,
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RefreshToken: cfg.RefreshToken,
			Scope:        cfg.Scope,
			RealmID:      cfg.RealmID,
			Entities:     cfg.Objects,
			QueryPerPage: cfg.QueryPerPage,
		}
	case "notion":
		token := cfg.AccessToken
		if token == "" {
			token = cfg.Secret
		}
		return &pollproviders.NotionFetcher{
			BaseURL:      cfg.BaseURL,
			Token:        token,
			TokenURL:     cfg.TokenURL,
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RefreshToken: cfg.RefreshToken,
			Scope:        cfg.Scope,
			PageSize:     cfg.QueryPerPage,
		}
	default:
		return nil
	}
}
