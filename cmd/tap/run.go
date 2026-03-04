package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
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
		requireAdminToken := func(w http.ResponseWriter, r *http.Request) bool {
			if strings.TrimSpace(r.Header.Get("X-Admin-Token")) != adminToken {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return false
			}
			return true
		}

		mux.HandleFunc("POST /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
			if !requireAdminToken(w, r) {
				return
			}
			limit := 100
			if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
					limit = parsed
				}
			}
			replayed, err := dlqPublisher.Replay(r.Context(), limit, func(ctx context.Context, subject string, payload []byte, dedupID string) error {
				return publisher.PublishRaw(ctx, subject, payload, dedupID)
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"replayed": replayed})
		})
		mux.HandleFunc("GET /admin/poller-status", func(w http.ResponseWriter, r *http.Request) {
			if !requireAdminToken(w, r) {
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"generated_at": time.Now().UTC(),
				"pollers":      pollerStatuses.Snapshot(),
			})
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

func newPollerStatusEntry(provider, tenantID string, interval time.Duration, rateLimitPerSec float64, burst int) *pollerStatusEntry {
	return &pollerStatusEntry{
		snapshot: pollerStatusSnapshot{
			Provider:        provider,
			TenantID:        tenantID,
			Interval:        interval.String(),
			RateLimitPerSec: rateLimitPerSec,
			Burst:           burst,
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

func (r *pollerStatusRegistry) upsert(provider, tenantID string, interval time.Duration, rateLimitPerSec float64, burst int) *pollerStatusEntry {
	if r == nil {
		return nil
	}
	key := poller.StateKey(provider, tenantID)
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok {
		entry = newPollerStatusEntry(provider, tenantID, interval, rateLimitPerSec, burst)
		r.entries[key] = entry
		return entry
	}
	entry.mu.Lock()
	entry.snapshot.Interval = interval.String()
	entry.snapshot.RateLimitPerSec = rateLimitPerSec
	entry.snapshot.Burst = burst
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
			limiter, limitPerSec, burst := pollLimiter(target)
			statusEntry := statuses.upsert(fetcher.ProviderName(), target.TenantID, interval, limitPerSec, burst)
			stateKey := poller.StateKey(fetcher.ProviderName(), target.TenantID)

			p := &poller.Poller{
				Provider:    fetcher.ProviderName(),
				Interval:    interval,
				RateLimiter: limiter,
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
