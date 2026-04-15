package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
	"github.com/evalops/siphon/internal/health"
	"golang.org/x/time/rate"
)

type adminRouteDeps struct {
	ctx            context.Context
	cfg            config.Config
	logger         *slog.Logger
	metrics        *health.Metrics
	publisher      rawPublisher
	dlqPublisher   *dlq.Publisher
	pollerStatuses *pollerStatusRegistry
}

type rawPublisher interface {
	PublishRaw(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error
}

func registerAdminRoutes(mux *http.ServeMux, deps adminRouteDeps) (closer, error) {
	if mux == nil {
		return nil, fmt.Errorf("admin route mux is required")
	}
	if !adminEndpointsEnabled(deps.cfg.Server) {
		return nil, nil
	}
	adminMux := http.NewServeMux()

	logger := deps.logger
	if logger == nil {
		logger = slog.Default()
	}

	metrics := deps.metrics
	adminToken := strings.TrimSpace(deps.cfg.Server.AdminToken)
	adminTokenSecondary := strings.TrimSpace(deps.cfg.Server.AdminTokenSecondary)
	adminTokenRead := strings.TrimSpace(deps.cfg.Server.AdminTokenRead)
	adminTokenReplay := strings.TrimSpace(deps.cfg.Server.AdminTokenReplay)
	adminTokenCancel := strings.TrimSpace(deps.cfg.Server.AdminTokenCancel)
	adminReplayMaxLimit := deps.cfg.Server.AdminReplayMaxLimit
	if adminReplayMaxLimit <= 0 {
		adminReplayMaxLimit = maxReplayDLQLimit
	}
	adminRateLimitPerSec := deps.cfg.Server.AdminRateLimitPerSec
	if adminRateLimitPerSec <= 0 {
		adminRateLimitPerSec = 1
	}
	adminRateLimitBurst := deps.cfg.Server.AdminRateLimitBurst
	if adminRateLimitBurst <= 0 {
		adminRateLimitBurst = 1
	}
	adminAllowedCIDRs, err := parseAdminAllowedCIDRs(deps.cfg.Server.AdminAllowedCIDRs)
	if err != nil {
		return nil, fmt.Errorf("parse admin allowlist: %w", err)
	}
	adminMTLSRequired := deps.cfg.Server.AdminMTLSRequired
	adminMTLSClientCertHeader := strings.TrimSpace(deps.cfg.Server.AdminMTLSClientCertHeader)
	adminReplayJobTimeout := deps.cfg.Server.AdminReplayJobTimeout
	if adminReplayJobTimeout <= 0 {
		adminReplayJobTimeout = 5 * time.Minute
	}
	adminReplayMaxConcurrent := deps.cfg.Server.AdminReplayMaxConcurrent
	if adminReplayMaxConcurrent <= 0 {
		adminReplayMaxConcurrent = 1
	}
	adminReplayRequireReason := deps.cfg.Server.AdminReplayRequireReason
	adminReplayReasonMinLen := deps.cfg.Server.AdminReplayReasonMinLen
	if adminReplayReasonMinLen <= 0 {
		adminReplayReasonMinLen = 1
	}
	adminReplayMaxQueuedPerIP := deps.cfg.Server.AdminReplayMaxQueuedPerIP
	adminReplayMaxQueuedPerToken := deps.cfg.Server.AdminReplayMaxQueuedToken
	adminReplayStoreBackend := strings.ToLower(strings.TrimSpace(deps.cfg.Server.AdminReplayStoreBackend))
	adminReplaySQLitePath := strings.TrimSpace(deps.cfg.Server.AdminReplaySQLitePath)
	retryAfterSeconds := adminRetryAfterSeconds(adminRateLimitPerSec)
	adminLimiters := newAdminRateLimiterRegistry(rate.Limit(adminRateLimitPerSec), adminRateLimitBurst)
	replayJobs, err := newAdminReplayJobRegistryWithBackend(
		deps.cfg.Server.AdminReplayJobMaxJobs,
		deps.cfg.Server.AdminReplayJobTTL,
		adminReplayStoreBackend,
		adminReplaySQLitePath,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("initialize admin replay job registry: %w", err)
	}
	replaySlots := make(chan struct{}, adminReplayMaxConcurrent)
	observeAdmin := func(endpoint, outcome string, startedAt time.Time) {
		if metrics == nil {
			return
		}
		metrics.AdminRequestsTotal.WithLabelValues(endpoint, outcome).Inc()
		metrics.AdminRequestDurationSeconds.WithLabelValues(endpoint, outcome).Observe(time.Since(startedAt).Seconds())
	}
	observeReplayJob := func(stage string) {
		if metrics == nil {
			return
		}
		metrics.AdminReplayJobsTotal.WithLabelValues(stage).Inc()
	}

	type adminAccess struct {
		RequestID        string
		TokenSlot        string
		TokenFingerprint string
		UserAgent        string
		RequestIP        string
	}

	requireAdminAccess := func(endpoint, scope string, w http.ResponseWriter, r *http.Request) (adminAccess, bool) {
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
		authorized, tokenSlot := authorizeAdminTokenForScope(
			tokenHeader,
			scope,
			adminToken,
			adminTokenSecondary,
			adminTokenRead,
			adminTokenReplay,
			adminTokenCancel,
		)
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
			RequestID:        reqID,
			TokenSlot:        tokenSlot,
			TokenFingerprint: adminTokenFingerprint(tokenHeader),
			UserAgent:        userAgent,
			RequestIP:        requestIP,
		}, true
	}

	adminMux.HandleFunc("GET /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
		access, ok := requireAdminAccess(adminEndpointReplayDLQList, adminScopeRead, w, r)
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

	adminMux.HandleFunc("POST /admin/replay-dlq", func(w http.ResponseWriter, r *http.Request) {
		access, ok := requireAdminAccess(adminEndpointReplayDLQ, adminScopeReplay, w, r)
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
		operatorReason, err := parseAdminReason(
			r.Header.Get("X-Admin-Reason"),
			adminReplayRequireReason,
			adminReplayReasonMinLen,
		)
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
				"error", err.Error(),
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
			writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
			return
		}
		idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		job, idempotencyReused, idempotencyConflict, queueConflict, queueScope, queueCount := replayJobs.GetOrCreateWithGuards(adminReplayJobSnapshot{
			RequestedLimit: requestedLimit,
			EffectiveLimit: limit,
			MaxLimit:       adminReplayMaxLimit,
			Capped:         capped,
			DryRun:         dryRun,
		}, adminReplayJobCreateMeta{
			OperatorReason:          operatorReason,
			CreatorIP:               access.RequestIP,
			CreatorTokenFingerprint: access.TokenFingerprint,
			RequestID:               access.RequestID,
		}, idempotencyKey, adminReplayMaxQueuedPerIP, adminReplayMaxQueuedPerToken)
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
		if queueConflict {
			observeAdmin(adminEndpointReplayDLQ, adminOutcomeConflict, startedAt)
			observeReplayJob("queue_limited")
			logger.Warn("admin replay dlq queue limit conflict",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayDLQ,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"queue_scope", queueScope,
				"queue_count", queueCount,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
			writeAdminError(w, http.StatusConflict, access.RequestID, "replay queue limit exceeded for caller scope")
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
			if metrics != nil {
				metrics.AdminReplayJobsInFlight.Inc()
			}
			defer func() {
				if metrics != nil {
					metrics.AdminReplayJobsInFlight.Dec()
				}
				<-replaySlots
			}()
			if !replayJobs.MarkRunning(jobID) {
				return
			}
			if dryRun {
				pending, err := deps.dlqPublisher.Pending()
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
			replayCtx, cancelReplay := context.WithTimeout(deps.ctx, adminReplayJobTimeout)
			defer cancelReplay()
			replayed, err := deps.dlqPublisher.Replay(replayCtx, limit, func(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error {
				return deps.publisher.PublishRaw(ctx, subject, payload, dedupID, requestID)
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
			"operator_reason", operatorReason,
			"capped", capped,
			"duration_ms", time.Since(startedAt).Milliseconds(),
		)
	})
	adminMux.HandleFunc("GET /admin/replay-dlq/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		access, ok := requireAdminAccess(adminEndpointReplayStatus, adminScopeRead, w, r)
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
	adminMux.HandleFunc("DELETE /admin/replay-dlq/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		access, ok := requireAdminAccess(adminEndpointReplayCancel, adminScopeCancel, w, r)
		if !ok {
			return
		}
		startedAt := time.Now()
		jobID := strings.TrimSpace(r.PathValue("job_id"))
		if jobID == "" {
			observeAdmin(adminEndpointReplayCancel, adminOutcomeBadRequest, startedAt)
			writeAdminError(w, http.StatusBadRequest, access.RequestID, "job_id is required")
			return
		}
		cancelReason, err := parseAdminReason(
			r.Header.Get("X-Admin-Reason"),
			adminReplayRequireReason,
			adminReplayReasonMinLen,
		)
		if err != nil {
			observeAdmin(adminEndpointReplayCancel, adminOutcomeBadRequest, startedAt)
			writeAdminError(w, http.StatusBadRequest, access.RequestID, err.Error())
			return
		}
		job, found, cancelled := replayJobs.CancelQueued(jobID, cancelReason)
		if !found {
			observeAdmin(adminEndpointReplayCancel, adminOutcomeNotFound, startedAt)
			logger.Warn("admin replay dlq cancel missing",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayCancel,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"job_id", jobID,
				"cancel_reason", cancelReason,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
			writeAdminError(w, http.StatusNotFound, access.RequestID, "replay job not found")
			return
		}
		if !cancelled {
			observeAdmin(adminEndpointReplayCancel, adminOutcomeConflict, startedAt)
			logger.Warn("admin replay dlq cancel conflict",
				"path", r.URL.Path,
				"method", r.Method,
				"endpoint", adminEndpointReplayCancel,
				"request_id", access.RequestID,
				"token_slot", access.TokenSlot,
				"requester_ip", access.RequestIP,
				"user_agent", access.UserAgent,
				"job_id", jobID,
				"status", job.Status,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
			writeAdminError(w, http.StatusConflict, access.RequestID, "replay job cannot be cancelled once running or completed")
			return
		}
		observeReplayJob("cancelled")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id": access.RequestID,
			"job":        job,
		})
		observeAdmin(adminEndpointReplayCancel, adminOutcomeSuccess, startedAt)
		logger.Info("admin replay dlq cancelled",
			"path", r.URL.Path,
			"method", r.Method,
			"endpoint", adminEndpointReplayCancel,
			"request_id", access.RequestID,
			"token_slot", access.TokenSlot,
			"requester_ip", access.RequestIP,
			"user_agent", access.UserAgent,
			"job_id", jobID,
			"duration_ms", time.Since(startedAt).Milliseconds(),
		)
	})
	adminMux.HandleFunc("GET /admin/poller-status", func(w http.ResponseWriter, r *http.Request) {
		access, ok := requireAdminAccess(adminEndpointPollerStatus, adminScopeRead, w, r)
		if !ok {
			return
		}
		startedAt := time.Now()
		providerFilter := strings.TrimSpace(r.URL.Query().Get("provider"))
		tenantFilter := strings.TrimSpace(r.URL.Query().Get("tenant"))
		statuses := deps.pollerStatuses.SnapshotFiltered(providerFilter, tenantFilter)
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

	connectPath, connectHandler := newTapAdminConnectHandler(adminMux)
	mux.Handle(connectPath, connectHandler)
	mux.Handle("/admin/", adminMux)

	return replayJobs, nil
}
