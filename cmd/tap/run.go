package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
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

	startConfiguredPollers(ctx, cfg, publisher, logger)

	ingressServer := ingress.NewServer(cfg, publisher, metrics, logger)
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

type pollSink struct {
	publisher cloudEventPublisher
}

type cloudEventPublisher interface {
	Publish(ctx context.Context, event cloudevents.Event, dedupID string) (string, error)
}

func (s pollSink) Publish(ctx context.Context, evt normalize.NormalizedEvent, dedupID string) error {
	ce, err := normalize.ToCloudEvent(evt)
	if err != nil {
		return err
	}
	_, err = s.publisher.Publish(ctx, ce, dedupID)
	return err
}

func startConfiguredPollers(ctx context.Context, cfg config.Config, publisher cloudEventPublisher, logger *slog.Logger) {
	checkpointStore := store.NewInMemoryCheckpointStore()
	snapshotStore := store.NewInMemorySnapshotStore()
	sink := pollSink{publisher: publisher}

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
			BaseURL: cfg.BaseURL,
			Token:   token,
			Objects: cfg.Objects,
			Limit:   cfg.QueryPerPage,
		}
	case "salesforce":
		token := cfg.AccessToken
		if token == "" {
			token = cfg.Secret
		}
		return &pollproviders.SalesforceFetcher{
			BaseURL:      cfg.BaseURL,
			AccessToken:  token,
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
			BaseURL:  cfg.BaseURL,
			Token:    token,
			PageSize: cfg.QueryPerPage,
		}
	default:
		return nil
	}
}
