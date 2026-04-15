package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
	"github.com/evalops/siphon/internal/health"
	"github.com/evalops/siphon/internal/ingress"
	"github.com/evalops/siphon/internal/publish"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type readiness interface {
	Ready() error
}

func readinessChecks(checks ...readiness) func() error {
	filtered := make([]readiness, 0, len(checks))
	for _, check := range checks {
		if check == nil {
			continue
		}
		filtered = append(filtered, check)
	}
	return func() error {
		for _, check := range filtered {
			if err := check.Ready(); err != nil {
				return err
			}
		}
		return nil
	}
}

const (
	defaultReplayDLQLimit  = 100
	maxReplayDLQLimit      = 2000
	defaultReplayListLimit = 50
	maxReplayListLimit     = 500

	adminScopeRead   = "read"
	adminScopeReplay = "replay"
	adminScopeCancel = "cancel"

	adminEndpointReplayDLQ     = "replay_dlq"
	adminEndpointReplayDLQList = "replay_dlq_list"
	adminEndpointReplayStatus  = "replay_dlq_status"
	adminEndpointReplayCancel  = "replay_dlq_cancel"
	adminEndpointPollerStatus  = "poller_status"
	adminOutcomeSuccess        = "success"
	adminOutcomeConflict       = "conflict"
	adminOutcomeUnauthorized   = "unauthorized"
	adminOutcomeForbidden      = "forbidden"
	adminOutcomeNotFound       = "not_found"
	adminOutcomeRateLimited    = "rate_limited"
	adminOutcomeBadRequest     = "bad_request"
	adminOutcomeInternalError  = "error"

	adminReplayJobStatusQueued    = "queued"
	adminReplayJobStatusRunning   = "running"
	adminReplayJobStatusSucceeded = "succeeded"
	adminReplayJobStatusFailed    = "failed"
	adminReplayJobStatusCancelled = "cancelled"
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
	startConfiguredPollers(ctx, cfg, publisher, dlqPublisher, logger, checkpointStore, snapshotStore, metrics, pollerStatuses)
	startPollerHealthMonitor(ctx, pollerStatuses, metrics)

	ingressServer := ingress.NewServer(cfg, publisher, metrics, logger)
	ingressServer.SetDLQRecorder(dlqPublisher)
	mux := http.NewServeMux()
	mux.Handle("/", ingressServer.Routes())
	mux.Handle("GET /livez", health.LivenessHandler())
	var readyChecks []readiness
	if rd, ok := any(publisher).(readiness); ok {
		readyChecks = append(readyChecks, rd)
	}
	if rd, ok := any(clickhouseSink).(readiness); ok {
		readyChecks = append(readyChecks, rd)
	}
	mux.Handle("GET /readyz", health.ReadinessHandler(readinessChecks(readyChecks...)))
	mux.Handle("GET /metrics", promhttp.Handler())
	adminCloser, err := registerAdminRoutes(mux, adminRouteDeps{
		ctx:            ctx,
		cfg:            cfg,
		logger:         logger,
		metrics:        metrics,
		publisher:      publisher,
		dlqPublisher:   dlqPublisher,
		pollerStatuses: pollerStatuses,
	})
	if err != nil {
		return err
	}
	if adminCloser != nil {
		defer adminCloser.Close()
	}

	httpServer := ingressServer.HTTPServer(mux)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("siphon started", "addr", httpServer.Addr, "base_path", cfg.Server.BasePath)
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
