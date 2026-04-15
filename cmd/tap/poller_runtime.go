package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
	"github.com/evalops/siphon/internal/health"
	"github.com/evalops/siphon/internal/normalize"
	"github.com/evalops/siphon/internal/poller"
	pollproviders "github.com/evalops/siphon/internal/poller/providers"
	"github.com/evalops/siphon/internal/store"
	"golang.org/x/time/rate"
)

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

func startPollerHealthMonitor(ctx context.Context, statuses *pollerStatusRegistry, metrics *health.Metrics) {
	if statuses == nil || metrics == nil || metrics.PollerStuck == nil || metrics.PollerConsecutiveFailures == nil {
		return
	}
	update := func() {
		now := time.Now().UTC()
		snapshots := statuses.Snapshot()
		metrics.PollerStuck.Reset()
		metrics.PollerConsecutiveFailures.Reset()
		for _, snapshot := range snapshots {
			provider := strings.TrimSpace(snapshot.Provider)
			tenant := strings.TrimSpace(snapshot.TenantID)
			metrics.PollerConsecutiveFailures.WithLabelValues(provider, tenant).Set(float64(snapshot.ConsecutiveFailures))
			if pollerLooksStuck(snapshot, now) {
				metrics.PollerStuck.WithLabelValues(provider, tenant).Set(1)
				continue
			}
			metrics.PollerStuck.WithLabelValues(provider, tenant).Set(0)
		}
	}
	update()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				update()
			}
		}
	}()
}

func pollerLooksStuck(snapshot pollerStatusSnapshot, now time.Time) bool {
	if snapshot.FailureBudget > 0 && snapshot.ConsecutiveFailures >= snapshot.FailureBudget {
		return true
	}
	if snapshot.LastRunAt.IsZero() {
		return false
	}
	interval := time.Minute
	if parsed, err := time.ParseDuration(strings.TrimSpace(snapshot.Interval)); err == nil && parsed > 0 {
		interval = parsed
	}
	staleAfter := interval * 3
	reference := snapshot.LastSuccessAt
	if reference.IsZero() {
		reference = snapshot.LastRunAt
	}
	if reference.IsZero() {
		return false
	}
	return now.UTC().Sub(reference.UTC()) > staleAfter
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

func startConfiguredPollers(ctx context.Context, cfg config.Config, publisher cloudEventPublisher, dlqPublisher *dlq.Publisher, logger *slog.Logger, checkpointStore store.CheckpointStore, snapshotStore store.SnapshotStore, metrics *health.Metrics, statuses *pollerStatusRegistry) {
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

						cycleStats, err := poller.RunCycleWithStats(ctx, fetcher, checkpointStore, snapshotStore, sink, tenantID)
						checkpointAfter, _ := checkpointStore.Get(stateKey)
						if err != nil {
							statusEntry.markError(err, checkpointAfter)
							return err
						}
						if metrics != nil {
							provider := fetcher.ProviderName()
							tenant := strings.TrimSpace(tenantID)
							metrics.PollerFetchRequestsTotal.WithLabelValues(provider, tenant).Add(float64(cycleStats.Fetch.Requests))
							metrics.PollerFetchPagesTotal.WithLabelValues(provider, tenant).Add(float64(cycleStats.Fetch.Pages))
							if cycleStats.Fetch.Truncated {
								metrics.PollerFetchTruncatedTotal.WithLabelValues(provider, tenant).Inc()
							}
						}
						if cycleStats.Fetch.Truncated {
							logger.Warn(
								"poller fetch capped by budget",
								"provider", fetcher.ProviderName(),
								"tenant", tenantID,
								"fetch_requests", cycleStats.Fetch.Requests,
								"fetch_pages", cycleStats.Fetch.Pages,
								"fetched_entities", cycleStats.FetchedEntities,
								"published_entities", cycleStats.PublishedEntities,
							)
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
			MaxPages:     cfg.PollMaxPages,
			MaxRequests:  cfg.PollMaxRequests,
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
			MaxPages:     cfg.PollMaxPages,
			MaxRequests:  cfg.PollMaxRequests,
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
			MaxPages:     cfg.PollMaxPages,
			MaxRequests:  cfg.PollMaxRequests,
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
			MaxPages:     cfg.PollMaxPages,
			MaxRequests:  cfg.PollMaxRequests,
		}
	default:
		return nil
	}
}
