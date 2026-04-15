package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

	"github.com/evalops/siphon/config"
	"golang.org/x/time/rate"
)

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
	case adminReplayJobStatusQueued, adminReplayJobStatusRunning, adminReplayJobStatusSucceeded, adminReplayJobStatusFailed, adminReplayJobStatusCancelled:
		return filter, nil
	default:
		return "", fmt.Errorf("invalid status %q: must be one of queued|running|succeeded|failed|cancelled", strings.TrimSpace(raw))
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

func authorizeAdminTokenForScope(actual, scope, globalPrimary, globalSecondary, readToken, replayToken, cancelToken string) (bool, string) {
	if ok, slot := authorizeAdminToken(actual, globalPrimary, globalSecondary); ok {
		return true, slot
	}
	scope = strings.ToLower(strings.TrimSpace(scope))
	switch scope {
	case adminScopeRead:
		if secureTokenEqual(actual, readToken) {
			return true, "read"
		}
		if secureTokenEqual(actual, replayToken) {
			return true, "replay"
		}
		if secureTokenEqual(actual, cancelToken) {
			return true, "cancel"
		}
	case adminScopeReplay:
		if secureTokenEqual(actual, replayToken) {
			return true, "replay"
		}
	case adminScopeCancel:
		if secureTokenEqual(actual, cancelToken) {
			return true, "cancel"
		}
	default:
		return false, ""
	}
	return false, ""
}

func adminEndpointsEnabled(serverCfg config.ServerConfig) bool {
	return strings.TrimSpace(serverCfg.AdminToken) != "" ||
		strings.TrimSpace(serverCfg.AdminTokenSecondary) != "" ||
		strings.TrimSpace(serverCfg.AdminTokenRead) != "" ||
		strings.TrimSpace(serverCfg.AdminTokenReplay) != "" ||
		strings.TrimSpace(serverCfg.AdminTokenCancel) != ""
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

func parseAdminReason(raw string, required bool, minLen int) (string, error) {
	reason := strings.TrimSpace(raw)
	if reason == "" {
		if required {
			return "", fmt.Errorf("X-Admin-Reason header is required")
		}
		return "", nil
	}
	if minLen <= 0 {
		minLen = 1
	}
	if len(reason) < minLen {
		return "", fmt.Errorf("X-Admin-Reason must be at least %d characters", minLen)
	}
	return reason, nil
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
	RequestID      string    `json:"request_id,omitempty"`
	OperatorReason string    `json:"operator_reason,omitempty"`
	CancelReason   string    `json:"cancel_reason,omitempty"`
	Error          string    `json:"error,omitempty"`
}

type adminReplayJob struct {
	snapshot                adminReplayJobSnapshot
	idempotencyKey          string
	creatorIP               string
	creatorTokenFingerprint string
	updatedAt               time.Time
}

func adminReplayJobRequestEquivalent(existing, requested adminReplayJobSnapshot) bool {
	return existing.RequestedLimit == requested.RequestedLimit &&
		existing.EffectiveLimit == requested.EffectiveLimit &&
		existing.MaxLimit == requested.MaxLimit &&
		existing.Capped == requested.Capped &&
		existing.DryRun == requested.DryRun
}

type adminReplayJobCreateMeta struct {
	OperatorReason          string
	CreatorIP               string
	CreatorTokenFingerprint string
	RequestID               string
}

type adminReplayJobRegistry struct {
	mu            sync.RWMutex
	jobs          map[string]*adminReplayJob
	byIdempotency map[string]string
	ttl           time.Duration
	maxJobs       int
	sequence      uint64
	logger        *slog.Logger
	sqliteStore   *adminReplayJobSQLiteStore
}

func newAdminReplayJobRegistry(maxJobs int, ttl time.Duration) *adminReplayJobRegistry {
	registry, _ := newAdminReplayJobRegistryWithBackend(maxJobs, ttl, "memory", "", nil)
	return registry
}

func newAdminReplayJobRegistryWithBackend(maxJobs int, ttl time.Duration, backend, sqlitePath string, logger *slog.Logger) (*adminReplayJobRegistry, error) {
	if maxJobs <= 0 {
		maxJobs = 512
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}
	registry := &adminReplayJobRegistry{
		jobs:          make(map[string]*adminReplayJob),
		byIdempotency: make(map[string]string),
		ttl:           ttl,
		maxJobs:       maxJobs,
		logger:        logger,
	}
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "", "memory":
		return registry, nil
	case "sqlite":
		store, err := newAdminReplayJobSQLiteStore(sqlitePath)
		if err != nil {
			return nil, err
		}
		registry.sqliteStore = store
		loadedJobs, err := store.Load()
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		now := time.Now().UTC()
		for _, job := range loadedJobs {
			if job == nil || strings.TrimSpace(job.snapshot.JobID) == "" {
				continue
			}
			registry.jobs[job.snapshot.JobID] = job
			if job.idempotencyKey != "" {
				registry.byIdempotency[job.idempotencyKey] = job.snapshot.JobID
			}
			if seq := adminReplaySequenceFromJobID(job.snapshot.JobID); seq > registry.sequence {
				registry.sequence = seq
			}
		}
		registry.cleanupLocked(now)
		return registry, nil
	default:
		return nil, fmt.Errorf("unsupported admin replay registry backend %q", backend)
	}
}

func adminReplaySequenceFromJobID(jobID string) uint64 {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return 0
	}
	parts := strings.Split(jobID, "_")
	if len(parts) < 3 {
		return 0
	}
	seq, err := strconv.ParseUint(strings.TrimSpace(parts[len(parts)-1]), 10, 64)
	if err != nil {
		return 0
	}
	return seq
}

func (r *adminReplayJobRegistry) Close() error {
	if r == nil || r.sqliteStore == nil {
		return nil
	}
	return r.sqliteStore.Close()
}

func (r *adminReplayJobRegistry) Create(base adminReplayJobSnapshot, idempotencyKey string) adminReplayJobSnapshot {
	job, _, _, _, _, _ := r.GetOrCreateWithGuards(base, adminReplayJobCreateMeta{}, idempotencyKey, 0, 0)
	return job
}

func (r *adminReplayJobRegistry) GetOrCreate(base adminReplayJobSnapshot, idempotencyKey string) (adminReplayJobSnapshot, bool, bool) {
	job, reused, conflict, _, _, _ := r.GetOrCreateWithGuards(base, adminReplayJobCreateMeta{}, idempotencyKey, 0, 0)
	return job, reused, conflict
}

func (r *adminReplayJobRegistry) GetOrCreateWithGuards(
	base adminReplayJobSnapshot,
	meta adminReplayJobCreateMeta,
	idempotencyKey string,
	maxQueuedPerIP int,
	maxQueuedPerToken int,
) (adminReplayJobSnapshot, bool, bool, bool, string, int) {
	if r == nil {
		return adminReplayJobSnapshot{}, false, false, false, "", 0
	}
	now := time.Now().UTC()
	meta.OperatorReason = strings.TrimSpace(meta.OperatorReason)
	meta.CreatorIP = strings.TrimSpace(meta.CreatorIP)
	meta.CreatorTokenFingerprint = strings.TrimSpace(meta.CreatorTokenFingerprint)
	meta.RequestID = strings.TrimSpace(meta.RequestID)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	if idempotencyKey != "" {
		if existing, found := r.getByIdempotencyKeyLocked(idempotencyKey); found {
			if adminReplayJobRequestEquivalent(existing, base) {
				return existing, true, false, false, "", 0
			}
			return existing, false, true, false, "", 0
		}
	}
	if maxQueuedPerIP > 0 {
		count := r.countQueuedByIPLocked(meta.CreatorIP)
		if count >= maxQueuedPerIP {
			return adminReplayJobSnapshot{}, false, false, true, "ip", count
		}
	}
	if maxQueuedPerToken > 0 && meta.CreatorTokenFingerprint != "" {
		count := r.countQueuedByTokenLocked(meta.CreatorTokenFingerprint)
		if count >= maxQueuedPerToken {
			return adminReplayJobSnapshot{}, false, false, true, "token", count
		}
	}
	r.sequence++
	jobID := fmt.Sprintf("replay_%d_%d", now.UnixNano(), r.sequence)
	base.JobID = jobID
	base.Status = adminReplayJobStatusQueued
	base.CreatedAt = now
	base.OperatorReason = meta.OperatorReason
	base.RequestID = meta.RequestID
	job := &adminReplayJob{
		snapshot:                base,
		idempotencyKey:          idempotencyKey,
		creatorIP:               meta.CreatorIP,
		creatorTokenFingerprint: meta.CreatorTokenFingerprint,
		updatedAt:               now,
	}
	r.jobs[jobID] = job
	if job.idempotencyKey != "" {
		r.byIdempotency[job.idempotencyKey] = jobID
	}
	r.persistJobLocked(job)
	return base, false, false, false, "", 0
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

func (r *adminReplayJobRegistry) countQueuedByIPLocked(ip string) int {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return 0
	}
	count := 0
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if job.snapshot.Status == adminReplayJobStatusQueued && job.creatorIP == ip {
			count++
		}
	}
	return count
}

func (r *adminReplayJobRegistry) countQueuedByTokenLocked(tokenFingerprint string) int {
	tokenFingerprint = strings.TrimSpace(tokenFingerprint)
	if tokenFingerprint == "" {
		return 0
	}
	count := 0
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if job.snapshot.Status == adminReplayJobStatusQueued && job.creatorTokenFingerprint == tokenFingerprint {
			count++
		}
	}
	return count
}

func (r *adminReplayJobRegistry) persistJobLocked(job *adminReplayJob) {
	if r == nil || r.sqliteStore == nil || job == nil {
		return
	}
	if err := r.sqliteStore.Upsert(job); err != nil {
		if r.logger != nil {
			r.logger.Warn("persist admin replay job", "job_id", job.snapshot.JobID, "error", err)
		}
	}
}

func (r *adminReplayJobRegistry) deletePersistedJobLocked(jobID string) {
	if r == nil || r.sqliteStore == nil {
		return
	}
	if err := r.sqliteStore.Delete(jobID); err != nil {
		if r.logger != nil {
			r.logger.Warn("delete persisted admin replay job", "job_id", jobID, "error", err)
		}
	}
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
		adminReplayJobStatusCancelled: 0,
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

func (r *adminReplayJobRegistry) MarkRunning(jobID string) bool {
	jobID = strings.TrimSpace(jobID)
	if r == nil || jobID == "" {
		return false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	job := r.jobs[jobID]
	if job == nil {
		return false
	}
	if job.snapshot.Status != adminReplayJobStatusQueued {
		return false
	}
	job.snapshot.Status = adminReplayJobStatusRunning
	job.snapshot.StartedAt = now
	job.updatedAt = now
	r.persistJobLocked(job)
	return true
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

func (r *adminReplayJobRegistry) CancelQueued(jobID, reason string) (adminReplayJobSnapshot, bool, bool) {
	jobID = strings.TrimSpace(jobID)
	reason = strings.TrimSpace(reason)
	if r == nil || jobID == "" {
		return adminReplayJobSnapshot{}, false, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)
	job := r.jobs[jobID]
	if job == nil {
		return adminReplayJobSnapshot{}, false, false
	}
	if job.snapshot.Status != adminReplayJobStatusQueued {
		return job.snapshot, true, false
	}
	job.snapshot.Status = adminReplayJobStatusCancelled
	job.snapshot.CompletedAt = now
	job.snapshot.CancelReason = reason
	if reason == "" {
		job.snapshot.Error = "cancelled by operator"
	} else {
		job.snapshot.Error = "cancelled by operator: " + reason
	}
	job.updatedAt = now
	r.persistJobLocked(job)
	return job.snapshot, true, true
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
	r.persistJobLocked(job)
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
			r.deletePersistedJobLocked(jobID)
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
		r.deletePersistedJobLocked(oldestID)
	}
}
