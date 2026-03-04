package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
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
	defaultReplayDLQLimit  = 100
	maxReplayDLQLimit      = 2000
	defaultReplayListLimit = 50
	maxReplayListLimit     = 500

	adminEndpointReplayDLQ     = "replay_dlq"
	adminEndpointReplayDLQList = "replay_dlq_list"
	adminEndpointReplayStatus  = "replay_dlq_status"
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
	startPollerHealthMonitor(ctx, pollerStatuses, metrics)

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
		adminAllowedCIDRs, err := parseAdminAllowedCIDRs(cfg.Server.AdminAllowedCIDRs)
		if err != nil {
			return fmt.Errorf("parse admin allowlist: %w", err)
		}
		adminMTLSRequired := cfg.Server.AdminMTLSRequired
		adminMTLSClientCertHeader := strings.TrimSpace(cfg.Server.AdminMTLSClientCertHeader)
		adminReplayJobTimeout := cfg.Server.AdminReplayJobTimeout
		if adminReplayJobTimeout <= 0 {
			adminReplayJobTimeout = 5 * time.Minute
		}
		adminReplayMaxConcurrent := cfg.Server.AdminReplayMaxConcurrent
		if adminReplayMaxConcurrent <= 0 {
			adminReplayMaxConcurrent = 1
		}
		retryAfterSeconds := adminRetryAfterSeconds(adminRateLimitPerSec)
		adminLimiters := newAdminRateLimiterRegistry(rate.Limit(adminRateLimitPerSec), adminRateLimitBurst)
		replayJobs := newAdminReplayJobRegistry(cfg.Server.AdminReplayJobMaxJobs, cfg.Server.AdminReplayJobTTL)
		replaySlots := make(chan struct{}, adminReplayMaxConcurrent)
		observeAdmin := func(endpoint, outcome string, startedAt time.Time) {
			metrics.AdminRequestsTotal.WithLabelValues(endpoint, outcome).Inc()
			metrics.AdminRequestDurationSeconds.WithLabelValues(endpoint, outcome).Observe(time.Since(startedAt).Seconds())
		}
		observeReplayJob := func(stage string) {
			metrics.AdminReplayJobsTotal.WithLabelValues(stage).Inc()
		}
		type adminAccess struct {
			RequestID string
			TokenSlot string
			UserAgent string
			RequestIP string
		}
		requireAdminAccess := func(endpoint string, w http.ResponseWriter, r *http.Request) (adminAccess, bool) {
			reqID := requestID(r)
			userAgent := strings.TrimSpace(r.UserAgent())
			requestIP := requesterIP(r)
			w.Header().Set("X-Request-ID", reqID)
			startedAt := time.Now()
			if len(adminAllowedCIDRs) > 0 && !adminRequesterAllowed(requestIP, adminAllowedCIDRs) {
				observeAdmin(endpoint, adminOutcomeForbidden, startedAt)
				logger.Warn("admin request forbidden",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", endpoint,
					"request_id", reqID,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"reason", "cidr_allowlist_miss",
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusForbidden, reqID, "forbidden")
				return adminAccess{}, false
			}
			if adminMTLSRequired && !adminHasClientCert(r, adminMTLSClientCertHeader) {
				observeAdmin(endpoint, adminOutcomeForbidden, startedAt)
				logger.Warn("admin request forbidden",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", endpoint,
					"request_id", reqID,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"reason", "mtls_required",
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusForbidden, reqID, "mTLS client certificate required")
				return adminAccess{}, false
			}
			tokenHeader := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
			if ok, scope := adminLimiters.Allow(endpoint, requestIP, tokenHeader); !ok {
				observeAdmin(endpoint, adminOutcomeRateLimited, startedAt)
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
				logger.Warn("admin request rate limited",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", endpoint,
					"request_id", reqID,
					"requester_ip", requestIP,
					"user_agent", userAgent,
					"limit_scope", scope,
					"retry_after_seconds", retryAfterSeconds,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusTooManyRequests, reqID, "rate limit exceeded")
				return adminAccess{}, false
			}
			authorized, tokenSlot := authorizeAdminToken(tokenHeader, adminToken, adminTokenSecondary)
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
				return adminAccess{}, false
			}
			return adminAccess{
				RequestID: reqID,
				TokenSlot: tokenSlot,
				UserAgent: userAgent,
				RequestIP: requestIP,
			}, true
		}
		mux.HandleFunc("GET /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
			access, ok := requireAdminAccess(adminEndpointReplayDLQList, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			statusFilter, err := parseReplayJobStatusFilter(r.URL.Query().Get("status"))
			if err != nil {
				observeAdmin(adminEndpointReplayDLQList, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq list rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQList,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"status_raw", strings.TrimSpace(r.URL.Query().Get("status")),
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
				return
			}
			listLimit, err := parseReplayJobListLimit(r.URL.Query().Get("limit"))
			if err != nil {
				observeAdmin(adminEndpointReplayDLQList, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq list rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQList,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"limit_raw", strings.TrimSpace(r.URL.Query().Get("limit")),
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
				return
			}
			cursorRaw := strings.TrimSpace(r.URL.Query().Get("cursor"))
			cursorCreatedAt, cursorJobID, err := parseReplayJobCursor(cursorRaw)
			if err != nil {
				observeAdmin(adminEndpointReplayDLQList, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq list rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQList,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"cursor_raw", cursorRaw,
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
				return
			}
			jobs, summary, nextCursor := replayJobs.List(statusFilter, listLimit, cursorCreatedAt, cursorJobID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id":  access.RequestID,
				"status":      statusFilter,
				"limit":       listLimit,
				"cursor":      cursorRaw,
				"next_cursor": nextCursor,
				"count":       len(jobs),
				"summary":     summary,
				"jobs":        jobs,
			})
			observeAdmin(adminEndpointReplayDLQList, adminOutcomeSuccess, startedAt)
			logger.Info("admin replay dlq list fetched",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayDLQList,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"status_filter", statusFilter,
				"limit", listLimit,
				"cursor_present", cursorRaw != "",
				"next_cursor_present", nextCursor != "",
				"count", len(jobs),
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		})

		mux.HandleFunc("POST /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
			access, ok := requireAdminAccess(adminEndpointReplayDLQ, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			requestedLimit, limit, capped, err := parseReplayDLQLimit(r.URL.Query().Get("limit"), adminReplayMaxLimit)
			if err != nil {
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"requested_limit_raw", strings.TrimSpace(r.URL.Query().Get("limit")),
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
				return
			}
			dryRun, err := parseOptionalBool(r.URL.Query().Get("dry_run"), false)
			if err != nil {
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeBadRequest, startedAt)
				logger.Warn("admin replay dlq rejected",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"dry_run_raw", strings.TrimSpace(r.URL.Query().Get("dry_run")),
					"error", err.Error(),
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
				return
			}
			idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
			job, idempotencyReused, idempotencyConflict := replayJobs.GetOrCreate(adminReplayJobSnapshot{
				RequestedLimit: requestedLimit,
				EffectiveLimit: limit,
				MaxLimit:       adminReplayMaxLimit,
				Capped:         capped,
				DryRun:         dryRun,
			}, idempotencyKey)
			if idempotencyConflict {
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeConflict, startedAt)
				observeReplayJob("conflict")
				logger.Warn("admin replay dlq idempotency conflict",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"idempotency_key_fingerprint", adminTokenFingerprint(idempotencyKey),
					"job_id", job.JobID,
					"requested_limit", requestedLimit,
					"effective_limit", limit,
					"dry_run", dryRun,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusConflict, access.RequestID, "idempotency key already used for different replay parameters")
				return
			}
			if idempotencyReused {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"request_id":         access.RequestID,
					"idempotency_reused": true,
					"job":                job,
				})
				observeAdmin(adminEndpointReplayDLQ, adminOutcomeSuccess, startedAt)
				observeReplayJob("reused")
				logger.Info("admin replay dlq idempotency reused",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayDLQ,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"job_id", job.JobID,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				return
			}
			observeReplayJob("accepted")
			go func(jobID string, limit int, dryRun bool, tokenSlot, requestIP string) {
				replaySlots <- struct{}{}
				metrics.AdminReplayJobsInFlight.Inc()
				defer func() {
					metrics.AdminReplayJobsInFlight.Dec()
					<-replaySlots
				}()
				replayJobs.MarkRunning(jobID)
				if dryRun {
					pending, err := dlqPublisher.Pending()
					if err != nil {
						replayJobs.MarkFailed(jobID, err.Error())
						observeReplayJob("failed")
						logger.Error("admin replay dlq dry-run failed", "job_id", jobID, "token_slot", tokenSlot, "requester_ip", requestIP, "error", err.Error())
						return
					}
					wouldReplay := pending
					if wouldReplay > limit {
						wouldReplay = limit
					}
					replayJobs.MarkSucceeded(jobID, wouldReplay)
					observeReplayJob("succeeded")
					logger.Info("admin replay dlq dry-run completed", "job_id", jobID, "token_slot", tokenSlot, "requester_ip", requestIP, "would_replay", wouldReplay)
					return
				}
				replayCtx, cancelReplay := context.WithTimeout(ctx, adminReplayJobTimeout)
				defer cancelReplay()
				replayed, err := dlqPublisher.Replay(replayCtx, limit, func(ctx context.Context, subject string, payload []byte, dedupID string) error {
					return publisher.PublishRaw(ctx, subject, payload, dedupID)
				})
				if err != nil {
					replayJobs.MarkFailed(jobID, err.Error())
					observeReplayJob("failed")
					logger.Error("admin replay dlq job failed", "job_id", jobID, "token_slot", tokenSlot, "requester_ip", requestIP, "error", err.Error())
					return
				}
				replayJobs.MarkSucceeded(jobID, replayed)
				observeReplayJob("succeeded")
				logger.Info("admin replay dlq job completed", "job_id", jobID, "token_slot", tokenSlot, "requester_ip", requestIP, "replayed_count", replayed)
			}(job.JobID, job.EffectiveLimit, job.DryRun, access.TokenSlot, access.RequestIP)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id": access.RequestID,
				"job":        job,
			})
			observeAdmin(adminEndpointReplayDLQ, adminOutcomeSuccess, startedAt)
			logger.Info("admin replay dlq accepted",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayDLQ,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"job_id", job.JobID,
				"requested_limit", requestedLimit,
				"effective_limit", limit,
				"dry_run", dryRun,
				"capped", capped,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		})
		mux.HandleFunc("GET /admin/replay-dlq/{job_id}", func(w http.ResponseWriter, r *http.Request) {
			access, ok := requireAdminAccess(adminEndpointReplayStatus, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			jobID := strings.TrimSpace(r.PathValue("job_id"))
			if jobID == "" {
				observeAdmin(adminEndpointReplayStatus, adminOutcomeBadRequest, startedAt)
				writeAdminError(w, http.StatusBadRequest, access.RequestID, "job_id is required")
				return
			}
			job, found := replayJobs.Get(jobID)
			if !found {
				observeAdmin(adminEndpointReplayStatus, adminOutcomeNotFound, startedAt)
				logger.Warn("admin replay dlq status missing",
					"path", r.URL.Path,
					"method", r.Method,
					"endpoint", adminEndpointReplayStatus,
					"request_id", access.RequestID,
					"token_slot", access.TokenSlot,
					"requester_ip", access.RequestIP,
					"user_agent", access.UserAgent,
					"job_id", jobID,
					"duration_ms", time.Since(startedAt).Milliseconds(),
				)
				writeAdminError(w, http.StatusNotFound, access.RequestID, "replay job not found")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id": access.RequestID,
				"job":        job,
			})
			observeAdmin(adminEndpointReplayStatus, adminOutcomeSuccess, startedAt)
			logger.Info("admin replay dlq status fetched",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayStatus,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"job_id", jobID,
				"status", job.Status,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		})
		mux.HandleFunc("GET /admin/poller-status", func(w http.ResponseWriter, r *http.Request) {
			access, ok := requireAdminAccess(adminEndpointPollerStatus, w, r)
			if !ok {
				return
			}
			startedAt := time.Now()
			providerFilter := strings.TrimSpace(r.URL.Query().Get("provider"))
			tenantFilter := strings.TrimSpace(r.URL.Query().Get("tenant"))
			statuses := pollerStatuses.SnapshotFiltered(providerFilter, tenantFilter)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"generated_at": time.Now().UTC(),
				"request_id":   access.RequestID,
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
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
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

func parseReplayJobListLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultReplayListLimit, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid list limit %q: must be a positive integer", raw)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("invalid list limit %q: must be greater than 0", raw)
	}
	if parsed > maxReplayListLimit {
		parsed = maxReplayListLimit
	}
	return parsed, nil
}

func parseReplayJobStatusFilter(raw string) (string, error) {
	filter := strings.ToLower(strings.TrimSpace(raw))
	if filter == "" {
		return "", nil
	}
	switch filter {
	case adminReplayJobStatusQueued, adminReplayJobStatusRunning, adminReplayJobStatusSucceeded, adminReplayJobStatusFailed:
		return filter, nil
	default:
		return "", fmt.Errorf("invalid status %q: must be one of queued|running|succeeded|failed", strings.TrimSpace(raw))
	}
}

func parseReplayJobCursor(raw string) (time.Time, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor: malformed encoding")
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid cursor: malformed payload")
	}
	nanos, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor: bad timestamp")
	}
	jobID := strings.TrimSpace(parts[1])
	if jobID == "" {
		return time.Time{}, "", fmt.Errorf("invalid cursor: missing job id")
	}
	return time.Unix(0, nanos).UTC(), jobID, nil
}

func encodeReplayJobCursor(job adminReplayJobSnapshot) string {
	if job.CreatedAt.IsZero() || strings.TrimSpace(job.JobID) == "" {
		return ""
	}
	raw := fmt.Sprintf("%d|%s", job.CreatedAt.UTC().UnixNano(), strings.TrimSpace(job.JobID))
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
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

func parseOptionalBool(raw string, fallback bool) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid boolean %q", raw)
	}
	return value, nil
}

func parseAdminAllowedCIDRs(raw []string) ([]*net.IPNet, error) {
	out := make([]*net.IPNet, 0, len(raw))
	for _, cidr := range raw {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q", cidr)
		}
		out = append(out, network)
	}
	return out, nil
}

func adminRequesterAllowed(requestIP string, allow []*net.IPNet) bool {
	if len(allow) == 0 {
		return true
	}
	ip := net.ParseIP(strings.TrimSpace(requestIP))
	if ip == nil {
		return false
	}
	for _, cidr := range allow {
		if cidr != nil && cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func adminHasClientCert(r *http.Request, forwardedHeader string) bool {
	if r == nil {
		return false
	}
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
		return true
	}
	if strings.TrimSpace(forwardedHeader) == "" {
		return false
	}
	return strings.TrimSpace(r.Header.Get(forwardedHeader)) != ""
}

type adminRateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type adminRateLimiterRegistry struct {
	mu         sync.Mutex
	entries    map[string]*adminRateLimiterEntry
	limit      rate.Limit
	burst      int
	ttl        time.Duration
	maxEntries int
}

func newAdminRateLimiterRegistry(limit rate.Limit, burst int) *adminRateLimiterRegistry {
	if limit <= 0 {
		limit = rate.Limit(1)
	}
	if burst <= 0 {
		burst = 1
	}
	return &adminRateLimiterRegistry{
		entries:    make(map[string]*adminRateLimiterEntry),
		limit:      limit,
		burst:      burst,
		ttl:        15 * time.Minute,
		maxEntries: 10000,
	}
}

func (r *adminRateLimiterRegistry) Allow(endpoint, requestIP, token string) (bool, string) {
	if r == nil {
		return true, ""
	}
	ipKey := adminRateLimiterKey(endpoint, "ip", strings.TrimSpace(requestIP))
	if !r.allowKey(ipKey) {
		return false, "ip"
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return true, ""
	}
	tokenKey := adminRateLimiterKey(endpoint, "token", adminTokenFingerprint(token))
	if !r.allowKey(tokenKey) {
		return false, "token"
	}
	return true, ""
}

func (r *adminRateLimiterRegistry) allowKey(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	now := time.Now().UTC()
	r.mu.Lock()
	r.cleanupLocked(now)
	entry := r.entries[key]
	if entry == nil {
		entry = &adminRateLimiterEntry{
			limiter: rate.NewLimiter(r.limit, r.burst),
		}
		r.entries[key] = entry
	}
	entry.lastSeen = now
	limiter := entry.limiter
	r.mu.Unlock()
	return limiter.Allow()
}

func (r *adminRateLimiterRegistry) cleanupLocked(now time.Time) {
	if r == nil {
		return
	}
	cutoff := now.Add(-r.ttl)
	for key, entry := range r.entries {
		if entry == nil || entry.lastSeen.Before(cutoff) {
			delete(r.entries, key)
		}
	}
	for len(r.entries) > r.maxEntries {
		var oldestKey string
		var oldestTime time.Time
		for key, entry := range r.entries {
			if entry == nil {
				oldestKey = key
				break
			}
			if oldestKey == "" || entry.lastSeen.Before(oldestTime) {
				oldestKey = key
				oldestTime = entry.lastSeen
			}
		}
		if oldestKey == "" {
			break
		}
		delete(r.entries, oldestKey)
	}
}

func adminRateLimiterKey(endpoint, scope, identity string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = "unknown"
	}
	scope = strings.TrimSpace(scope)
	if scope == "" {
		scope = "unknown"
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		identity = "unknown"
	}
	return endpoint + "|" + scope + "|" + identity
}

func adminTokenFingerprint(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	encoded := hex.EncodeToString(sum[:])
	if len(encoded) > 16 {
		return encoded[:16]
	}
	return encoded
}

type adminReplayJobSnapshot struct {
	JobID          string    `json:"job_id"`
	Status         string    `json:"status"`
	RequestedLimit int       `json:"requested_limit,omitempty"`
	EffectiveLimit int       `json:"effective_limit"`
	MaxLimit       int       `json:"max_limit"`
	Capped         bool      `json:"capped"`
	DryRun         bool      `json:"dry_run"`
	CreatedAt      time.Time `json:"created_at"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	Replayed       int       `json:"replayed"`
	Error          string    `json:"error,omitempty"`
}

type adminReplayJob struct {
	snapshot       adminReplayJobSnapshot
	idempotencyKey string
	updatedAt      time.Time
}

func adminReplayJobRequestEquivalent(existing, requested adminReplayJobSnapshot) bool {
	return existing.RequestedLimit == requested.RequestedLimit &&
		existing.EffectiveLimit == requested.EffectiveLimit &&
		existing.MaxLimit == requested.MaxLimit &&
		existing.Capped == requested.Capped &&
		existing.DryRun == requested.DryRun
}

type adminReplayJobRegistry struct {
	mu            sync.RWMutex
	jobs          map[string]*adminReplayJob
	byIdempotency map[string]string
	ttl           time.Duration
	maxJobs       int
	sequence      uint64
}

func newAdminReplayJobRegistry(maxJobs int, ttl time.Duration) *adminReplayJobRegistry {
	if maxJobs <= 0 {
		maxJobs = 512
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &adminReplayJobRegistry{
		jobs:          make(map[string]*adminReplayJob),
		byIdempotency: make(map[string]string),
		ttl:           ttl,
		maxJobs:       maxJobs,
	}
}

func (r *adminReplayJobRegistry) Create(base adminReplayJobSnapshot, idempotencyKey string) adminReplayJobSnapshot {
	job, _, _ := r.GetOrCreate(base, idempotencyKey)
	return job
}

func (r *adminReplayJobRegistry) GetOrCreate(base adminReplayJobSnapshot, idempotencyKey string) (adminReplayJobSnapshot, bool, bool) {
	if r == nil {
		return adminReplayJobSnapshot{}, false, false
	}
	now := time.Now().UTC()
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	if idempotencyKey != "" {
		if existing, found := r.getByIdempotencyKeyLocked(idempotencyKey); found {
			if adminReplayJobRequestEquivalent(existing, base) {
				return existing, true, false
			}
			return existing, false, true
		}
	}
	r.sequence++
	jobID := fmt.Sprintf("replay_%d_%d", now.UnixNano(), r.sequence)
	base.JobID = jobID
	base.Status = adminReplayJobStatusQueued
	base.CreatedAt = now
	job := &adminReplayJob{
		snapshot:       base,
		idempotencyKey: idempotencyKey,
		updatedAt:      now,
	}
	r.jobs[jobID] = job
	if job.idempotencyKey != "" {
		r.byIdempotency[job.idempotencyKey] = jobID
	}
	return base, false, false
}

func (r *adminReplayJobRegistry) Get(jobID string) (adminReplayJobSnapshot, bool) {
	jobID = strings.TrimSpace(jobID)
	if r == nil || jobID == "" {
		return adminReplayJobSnapshot{}, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	r.cleanupLocked(now)
	job := r.jobs[jobID]
	if job == nil {
		r.mu.Unlock()
		return adminReplayJobSnapshot{}, false
	}
	snapshot := job.snapshot
	r.mu.Unlock()
	return snapshot, true
}

func (r *adminReplayJobRegistry) GetByIdempotencyKey(key string) (adminReplayJobSnapshot, bool) {
	key = strings.TrimSpace(key)
	if r == nil || key == "" {
		return adminReplayJobSnapshot{}, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	return r.getByIdempotencyKeyLocked(key)
}

func (r *adminReplayJobRegistry) getByIdempotencyKeyLocked(key string) (adminReplayJobSnapshot, bool) {
	jobID := r.byIdempotency[key]
	job := r.jobs[jobID]
	if job == nil {
		return adminReplayJobSnapshot{}, false
	}
	return job.snapshot, true
}

func (r *adminReplayJobRegistry) List(statusFilter string, limit int, cursorCreatedAt time.Time, cursorJobID string) ([]adminReplayJobSnapshot, map[string]int, string) {
	statusFilter = strings.ToLower(strings.TrimSpace(statusFilter))
	cursorJobID = strings.TrimSpace(cursorJobID)
	if limit <= 0 {
		limit = defaultReplayListLimit
	}
	if limit > maxReplayListLimit {
		limit = maxReplayListLimit
	}
	summary := map[string]int{
		adminReplayJobStatusQueued:    0,
		adminReplayJobStatusRunning:   0,
		adminReplayJobStatusSucceeded: 0,
		adminReplayJobStatusFailed:    0,
	}
	if r == nil {
		return []adminReplayJobSnapshot{}, summary, ""
	}
	now := time.Now().UTC()
	r.mu.Lock()
	r.cleanupLocked(now)
	out := make([]adminReplayJobSnapshot, 0, len(r.jobs))
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		status := strings.TrimSpace(job.snapshot.Status)
		if _, ok := summary[status]; ok {
			summary[status]++
		}
		if statusFilter != "" && status != statusFilter {
			continue
		}
		out = append(out, job.snapshot)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.JobID > right.JobID
		}
		return left.CreatedAt.After(right.CreatedAt)
	})

	if !cursorCreatedAt.IsZero() && cursorJobID != "" {
		filtered := make([]adminReplayJobSnapshot, 0, len(out))
		for _, job := range out {
			if replayJobComesAfterCursor(job, cursorCreatedAt, cursorJobID) {
				filtered = append(filtered, job)
			}
		}
		out = filtered
	}

	nextCursor := ""
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	if hasMore && len(out) == limit {
		last := out[len(out)-1]
		nextCursor = encodeReplayJobCursor(last)
	}
	return out, summary, nextCursor
}

func replayJobComesAfterCursor(candidate adminReplayJobSnapshot, cursorCreatedAt time.Time, cursorJobID string) bool {
	if candidate.CreatedAt.Before(cursorCreatedAt) {
		return true
	}
	if candidate.CreatedAt.After(cursorCreatedAt) {
		return false
	}
	return strings.TrimSpace(candidate.JobID) < strings.TrimSpace(cursorJobID)
}

func (r *adminReplayJobRegistry) MarkRunning(jobID string) {
	r.update(jobID, func(snapshot *adminReplayJobSnapshot) {
		if snapshot.Status != adminReplayJobStatusQueued {
			return
		}
		snapshot.Status = adminReplayJobStatusRunning
		snapshot.StartedAt = time.Now().UTC()
	})
}

func (r *adminReplayJobRegistry) MarkSucceeded(jobID string, replayed int) {
	r.update(jobID, func(snapshot *adminReplayJobSnapshot) {
		snapshot.Status = adminReplayJobStatusSucceeded
		snapshot.Replayed = replayed
		snapshot.CompletedAt = time.Now().UTC()
		snapshot.Error = ""
	})
}

func (r *adminReplayJobRegistry) MarkFailed(jobID, message string) {
	r.update(jobID, func(snapshot *adminReplayJobSnapshot) {
		snapshot.Status = adminReplayJobStatusFailed
		snapshot.CompletedAt = time.Now().UTC()
		snapshot.Error = strings.TrimSpace(message)
	})
}

func (r *adminReplayJobRegistry) update(jobID string, mutate func(snapshot *adminReplayJobSnapshot)) {
	jobID = strings.TrimSpace(jobID)
	if r == nil || jobID == "" || mutate == nil {
		return
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	job := r.jobs[jobID]
	if job == nil {
		return
	}
	mutate(&job.snapshot)
	job.updatedAt = time.Now().UTC()
}

func (r *adminReplayJobRegistry) cleanupLocked(now time.Time) {
	if r == nil {
		return
	}
	cutoff := now.Add(-r.ttl)
	for jobID, job := range r.jobs {
		if job == nil || job.updatedAt.Before(cutoff) {
			delete(r.jobs, jobID)
			if job != nil && job.idempotencyKey != "" {
				delete(r.byIdempotency, job.idempotencyKey)
			}
		}
	}
	for len(r.jobs) > r.maxJobs {
		var oldestID string
		var oldestTime time.Time
		for jobID, job := range r.jobs {
			if job == nil {
				oldestID = jobID
				break
			}
			if oldestID == "" || job.updatedAt.Before(oldestTime) {
				oldestID = jobID
				oldestTime = job.updatedAt
			}
		}
		if oldestID == "" {
			break
		}
		if job := r.jobs[oldestID]; job != nil && job.idempotencyKey != "" {
			delete(r.byIdempotency, job.idempotencyKey)
		}
		delete(r.jobs, oldestID)
	}
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
