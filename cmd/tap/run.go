package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

	startConfiguredPollers(ctx, cfg, publisher, dlqPublisher, logger, checkpointStore, snapshotStore)

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
		mux.HandleFunc("POST /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
			if strings.TrimSpace(r.Header.Get("X-Admin-Token")) != strings.TrimSpace(cfg.Server.AdminToken) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
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

func startConfiguredPollers(ctx context.Context, cfg config.Config, publisher cloudEventPublisher, dlqPublisher *dlq.Publisher, logger *slog.Logger, checkpointStore store.CheckpointStore, snapshotStore store.SnapshotStore) {
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

		fetcher := fetcherForProvider(providerName, pcfg)
		if fetcher == nil {
			logger.Warn("poll mode requested but provider poller not available", "provider", providerName)
			continue
		}

		interval := pcfg.PollInterval
		if interval <= 0 {
			interval = time.Minute
		}

		p := &poller.Poller{
			Provider:    fetcher.ProviderName(),
			Interval:    interval,
			RateLimiter: rate.NewLimiter(rate.Every(250*time.Millisecond), 1),
			Run: func(fetcher poller.Fetcher, tenantID string) poller.PollFn {
				return func(ctx context.Context) error {
					return poller.RunCycle(ctx, fetcher, checkpointStore, snapshotStore, sink, tenantID)
				}
			}(fetcher, pcfg.TenantID),
		}
		logger.Info("starting provider poller", "provider", providerName, "interval", interval.String())
		go func(p *poller.Poller) {
			p.Start(ctx)
		}(p)
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
