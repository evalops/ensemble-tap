package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
)

func TestRunStartsAndStopsWithReadyLifecycle(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"acme": {Mode: "webhook", Secret: "webhook-secret", TenantID: "tenant-1"},
		},
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{Port: port, BasePath: "/webhooks", MaxBodySize: 1 << 20},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	body := []byte(`{"id":"42","timestamp":"2026-03-03T14:22:00Z"}`)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+intToString(port)+"/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signGeneric(body, "webhook-secret"))
	req.Header.Set("X-Event-Type", "deal.updated")
	req.Header.Set("X-Event-Id", "evt_cmd_1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected webhook accepted, got %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunReadinessReflectsNATSDisconnect(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_READY",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{Port: port, BasePath: "/webhooks", MaxBodySize: 1 << 20},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	s.Shutdown()
	s.WaitForShutdown()

	if err := waitForStatusOrError(readyURL, http.StatusServiceUnavailable, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint did not reflect nats disconnect: %v", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestReadinessChecks(t *testing.T) {
	t.Run("all checks healthy", func(t *testing.T) {
		check := readinessChecks(
			testReadiness{err: nil},
			testReadiness{err: nil},
		)
		if err := check(); err != nil {
			t.Fatalf("expected readiness checks to pass, got %v", err)
		}
	})

	t.Run("returns first readiness error", func(t *testing.T) {
		check := readinessChecks(
			testReadiness{err: nil},
			testReadiness{err: fmt.Errorf("clickhouse not ready")},
			testReadiness{err: fmt.Errorf("should not be reached")},
		)
		if err := check(); err == nil || !strings.Contains(err.Error(), "clickhouse not ready") {
			t.Fatalf("expected clickhouse readiness error, got %v", err)
		}
	})

	t.Run("ignores nil checks", func(t *testing.T) {
		var nilCheck readiness
		check := readinessChecks(nilCheck)
		if err := check(); err != nil {
			t.Fatalf("expected nil readiness checks to pass, got %v", err)
		}
	})
}

func TestRunRejectsInvalidAdminConfig(t *testing.T) {
	err := run(context.Background(), config.Config{
		Server: config.ServerConfig{
			AdminTokenSecondary: "next-admin-token",
		},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatalf("expected invalid admin config to fail before runtime start")
	}
	if !strings.Contains(err.Error(), "validate runtime config") {
		t.Fatalf("expected validation error prefix, got %v", err)
	}
	if !strings.Contains(err.Error(), "admin_token_secondary requires") {
		t.Fatalf("expected secondary token validation error, got %v", err)
	}
}

func TestPollerLooksStuck(t *testing.T) {
	now := time.Now().UTC()
	if !pollerLooksStuck(pollerStatusSnapshot{
		Provider:            "notion",
		Interval:            "10s",
		FailureBudget:       3,
		ConsecutiveFailures: 3,
		LastRunAt:           now,
	}, now) {
		t.Fatalf("expected poller at failure budget to be marked stuck")
	}
	if !pollerLooksStuck(pollerStatusSnapshot{
		Provider:      "hubspot",
		Interval:      "5s",
		LastRunAt:     now.Add(-25 * time.Second),
		LastSuccessAt: now.Add(-25 * time.Second),
	}, now) {
		t.Fatalf("expected stale poller to be marked stuck")
	}
	if pollerLooksStuck(pollerStatusSnapshot{
		Provider:      "salesforce",
		Interval:      "30s",
		LastRunAt:     now.Add(-10 * time.Second),
		LastSuccessAt: now.Add(-10 * time.Second),
	}, now) {
		t.Fatalf("expected recently healthy poller to be unstuck")
	}
}

func TestRequesterIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/admin/poller-status", nil)
	req.RemoteAddr = "127.0.0.1:12345"

	if got := requesterIP(req); got != "127.0.0.1" {
		t.Fatalf("unexpected remote requester ip: %q", got)
	}

	req.Header.Set("X-Real-IP", "10.0.0.5")
	if got := requesterIP(req); got != "10.0.0.5" {
		t.Fatalf("unexpected x-real-ip requester ip: %q", got)
	}

	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.7")
	if got := requesterIP(req); got != "203.0.113.9" {
		t.Fatalf("unexpected forwarded requester ip: %q", got)
	}
}

func TestRequestID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/admin/poller-status", nil)
	req.Header.Set("X-Request-ID", "req-123")
	if got := requestID(req); got != "req-123" {
		t.Fatalf("unexpected request id from header: %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "http://example.com/admin/poller-status", nil)
	req.Header.Set("X-Correlation-ID", "corr-456")
	if got := requestID(req); got != "corr-456" {
		t.Fatalf("unexpected request id from correlation header: %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "http://example.com/admin/poller-status", nil)
	if got := requestID(req); !strings.HasPrefix(got, "admin-") {
		t.Fatalf("expected generated request id with admin- prefix, got %q", got)
	}
}

func TestPollerStatusRegistrySnapshotFiltered(t *testing.T) {
	registry := newPollerStatusRegistry()
	registry.upsert("notion", "tenant-a", 25*time.Millisecond, 9.0, 3, 5, 30*time.Second, 0.2)
	registry.upsert("hubspot", "tenant-b", 50*time.Millisecond, 4.0, 1, 7, 45*time.Second, 0.1)

	if got := registry.SnapshotFiltered("", ""); len(got) != 2 {
		t.Fatalf("expected 2 pollers without filter, got %d", len(got))
	}
	if got := registry.SnapshotFiltered("NOTION", ""); len(got) != 1 || got[0].Provider != "notion" {
		t.Fatalf("expected provider filter to be case-insensitive, got %+v", got)
	}
	if got := registry.SnapshotFiltered("", "tenant-b"); len(got) != 1 || got[0].TenantID != "tenant-b" {
		t.Fatalf("expected tenant filter to match tenant-b, got %+v", got)
	}
	if got := registry.SnapshotFiltered("notion", "tenant-b"); len(got) != 0 {
		t.Fatalf("expected empty result for mismatched provider+tenant filter, got %+v", got)
	}
}

func TestRunAdminReplayEndpointRequiresToken(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	const replayMaxLimit = 1500
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_REPLAY",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                port,
			BasePath:            "/webhooks",
			MaxBodySize:         1 << 20,
			AdminToken:          "test-admin-token",
			AdminTokenSecondary: "next-admin-token",
			AdminReplayMaxLimit: replayMaxLimit,
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logBuf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, testLogger)
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	type adminErrorResponse struct {
		RequestID string `json:"request_id"`
		Error     string `json:"error"`
	}
	readAdminError := func(resp *http.Response) adminErrorResponse {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read admin error response body: %v", err)
		}
		var out adminErrorResponse
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode admin error response body: %v body=%s", err, string(body))
		}
		return out
	}

	replayBaseURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq"
	replayURL := replayBaseURL + "?limit=1"
	reqNoToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqNoToken.Header.Set("X-Request-ID", "replay-unauth-1")
	reqNoToken.Header.Set("X-Forwarded-For", "203.0.113.50")
	reqNoToken.Header.Set("User-Agent", "tap-admin-test/unauth")
	respNoToken, err := http.DefaultClient.Do(reqNoToken)
	if err != nil {
		t.Fatalf("request replay without token: %v", err)
	}
	if got := strings.TrimSpace(respNoToken.Header.Get("X-Request-ID")); got != "replay-unauth-1" {
		t.Fatalf("expected unauthorized response to echo request id, got %q", got)
	}
	if got := strings.TrimSpace(respNoToken.Header.Get("Content-Type")); !strings.Contains(got, "application/json") {
		t.Fatalf("expected unauthorized response content-type application/json, got %q", got)
	}
	if respNoToken.StatusCode != http.StatusUnauthorized {
		_ = respNoToken.Body.Close()
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
	}
	unauthErr := readAdminError(respNoToken)
	if unauthErr.RequestID != "replay-unauth-1" || unauthErr.Error != "unauthorized" {
		t.Fatalf("unexpected unauthorized replay response payload: %+v", unauthErr)
	}
	reqInvalidLimit, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?limit=invalid", nil)
	reqInvalidLimit.Header.Set("X-Admin-Token", "test-admin-token")
	reqInvalidLimit.Header.Set("X-Request-ID", "replay-invalid-1")
	reqInvalidLimit.Header.Set("X-Forwarded-For", "203.0.113.51")
	reqInvalidLimit.Header.Set("User-Agent", "tap-admin-test/invalid")
	respInvalidLimit, err := http.DefaultClient.Do(reqInvalidLimit)
	if err != nil {
		t.Fatalf("request replay with invalid limit: %v", err)
	}
	if got := strings.TrimSpace(respInvalidLimit.Header.Get("X-Request-ID")); got != "replay-invalid-1" {
		t.Fatalf("expected invalid-limit response to echo request id, got %q", got)
	}
	if got := strings.TrimSpace(respInvalidLimit.Header.Get("Content-Type")); !strings.Contains(got, "application/json") {
		t.Fatalf("expected invalid-limit response content-type application/json, got %q", got)
	}
	if respInvalidLimit.StatusCode != http.StatusBadRequest {
		_ = respInvalidLimit.Body.Close()
		t.Fatalf("expected 400 for invalid replay limit, got %d", respInvalidLimit.StatusCode)
	}
	invalidErr := readAdminError(respInvalidLimit)
	if invalidErr.RequestID != "replay-invalid-1" || !strings.Contains(invalidErr.Error, "invalid limit") {
		t.Fatalf("unexpected invalid replay response payload: %+v", invalidErr)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}

	subject := "siphon.tap.replay.test.updated"
	payload := []byte(`{"id":"replay_1"}`)
	rec := dlq.Record{
		Stage:           "publish",
		Provider:        "test",
		Reason:          "manual replay test",
		OriginalSubject: subject,
		OriginalDedupID: "replay_1",
		OriginalPayload: payload,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal dlq record: %v", err)
	}
	msg := &nats.Msg{
		Subject: "siphon.dlq.publish.test",
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, "dlq_test_replay_1")
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish dlq message: %v", err)
	}

	replayedSub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe replay subject: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush nats connection: %v", err)
	}

	type replayJobSnapshot struct {
		JobID          string    `json:"job_id"`
		Status         string    `json:"status"`
		RequestedLimit int       `json:"requested_limit"`
		EffectiveLimit int       `json:"effective_limit"`
		MaxLimit       int       `json:"max_limit"`
		Capped         bool      `json:"capped"`
		DryRun         bool      `json:"dry_run"`
		CreatedAt      time.Time `json:"created_at"`
		StartedAt      time.Time `json:"started_at"`
		CompletedAt    time.Time `json:"completed_at"`
		Replayed       int       `json:"replayed"`
		Error          string    `json:"error"`
	}
	type replayJobEnvelope struct {
		RequestID         string            `json:"request_id"`
		IdempotencyReused bool              `json:"idempotency_reused"`
		Job               replayJobSnapshot `json:"job"`
	}
	readReplayJobEnvelope := func(resp *http.Response) replayJobEnvelope {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read replay job response body: %v", err)
		}
		var out replayJobEnvelope
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode replay job response body: %v body=%s", err, string(body))
		}
		return out
	}
	waitReplayJob := func(token, reqID, jobID string) replayJobSnapshot {
		t.Helper()
		statusURL := replayBaseURL + "/" + jobID
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			req, _ := http.NewRequest(http.MethodGet, statusURL, nil)
			req.Header.Set("X-Admin-Token", token)
			req.Header.Set("X-Request-ID", reqID)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request replay status: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				_ = resp.Body.Close()
				t.Fatalf("expected replay status 200, got %d", resp.StatusCode)
			}
			out := readReplayJobEnvelope(resp)
			if out.Job.JobID != jobID {
				t.Fatalf("expected replay status for %q, got %q", jobID, out.Job.JobID)
			}
			switch out.Job.Status {
			case adminReplayJobStatusSucceeded, adminReplayJobStatusFailed:
				return out.Job
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for replay job %q completion", jobID)
		return replayJobSnapshot{}
	}

	reqWithToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqWithToken.Header.Set("X-Admin-Token", "test-admin-token")
	reqWithToken.Header.Set("X-Request-ID", "replay-success-1")
	reqWithToken.Header.Set("X-Forwarded-For", "203.0.113.52")
	reqWithToken.Header.Set("User-Agent", "tap-admin-test/success")
	respWithToken, err := http.DefaultClient.Do(reqWithToken)
	if err != nil {
		t.Fatalf("request replay with token: %v", err)
	}
	if got := strings.TrimSpace(respWithToken.Header.Get("X-Request-ID")); got != "replay-success-1" {
		_ = respWithToken.Body.Close()
		t.Fatalf("expected accepted response to echo request id, got %q", got)
	}
	if respWithToken.StatusCode != http.StatusAccepted {
		_ = respWithToken.Body.Close()
		t.Fatalf("expected 202 with admin token, got %d", respWithToken.StatusCode)
	}
	accepted := readReplayJobEnvelope(respWithToken)
	if accepted.RequestID != "replay-success-1" {
		t.Fatalf("unexpected accepted replay request id: %+v", accepted)
	}
	if accepted.Job.JobID == "" || accepted.Job.EffectiveLimit != 1 || accepted.Job.MaxLimit != replayMaxLimit {
		t.Fatalf("unexpected accepted replay job payload: %+v", accepted.Job)
	}
	finished := waitReplayJob("test-admin-token", "replay-status-1", accepted.Job.JobID)
	if finished.Status != adminReplayJobStatusSucceeded || finished.Replayed != 1 {
		t.Fatalf("unexpected finished replay job: %+v", finished)
	}
	got, err := replayedSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected replayed message: %v", err)
	}
	if string(got.Data) != string(payload) {
		t.Fatalf("unexpected replay payload: %s", string(got.Data))
	}

	reqWithCap, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?limit=99999", nil)
	reqWithCap.Header.Set("X-Admin-Token", "next-admin-token")
	reqWithCap.Header.Set("X-Request-ID", "replay-cap-1")
	reqWithCap.Header.Set("X-Forwarded-For", "203.0.113.53")
	reqWithCap.Header.Set("User-Agent", "tap-admin-test/cap")
	respWithCap, err := http.DefaultClient.Do(reqWithCap)
	if err != nil {
		t.Fatalf("request replay with capped limit: %v", err)
	}
	if got := strings.TrimSpace(respWithCap.Header.Get("X-Request-ID")); got != "replay-cap-1" {
		_ = respWithCap.Body.Close()
		t.Fatalf("expected capped accepted response to echo request id, got %q", got)
	}
	if respWithCap.StatusCode != http.StatusAccepted {
		_ = respWithCap.Body.Close()
		t.Fatalf("expected 202 with capped limit, got %d", respWithCap.StatusCode)
	}
	cappedAccepted := readReplayJobEnvelope(respWithCap)
	if cappedAccepted.Job.EffectiveLimit != replayMaxLimit || !cappedAccepted.Job.Capped {
		t.Fatalf("unexpected capped accepted replay payload: %+v", cappedAccepted.Job)
	}
	cappedFinished := waitReplayJob("next-admin-token", "replay-status-cap-1", cappedAccepted.Job.JobID)
	if cappedFinished.Status != adminReplayJobStatusSucceeded {
		t.Fatalf("unexpected capped replay job status: %+v", cappedFinished)
	}

	reqDryRun, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?dry_run=true&limit=10", nil)
	reqDryRun.Header.Set("X-Admin-Token", "test-admin-token")
	reqDryRun.Header.Set("X-Request-ID", "replay-dry-1")
	reqDryRun.Header.Set("Idempotency-Key", "idem-replay-1")
	respDryRun, err := http.DefaultClient.Do(reqDryRun)
	if err != nil {
		t.Fatalf("request replay dry-run: %v", err)
	}
	if respDryRun.StatusCode != http.StatusAccepted {
		_ = respDryRun.Body.Close()
		t.Fatalf("expected 202 for dry-run replay, got %d", respDryRun.StatusCode)
	}
	dryRunAccepted := readReplayJobEnvelope(respDryRun)
	if !dryRunAccepted.Job.DryRun {
		t.Fatalf("expected dry-run replay job")
	}
	reqDryRunReuse, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?dry_run=true&limit=10", nil)
	reqDryRunReuse.Header.Set("X-Admin-Token", "test-admin-token")
	reqDryRunReuse.Header.Set("X-Request-ID", "replay-dry-2")
	reqDryRunReuse.Header.Set("Idempotency-Key", "idem-replay-1")
	respDryRunReuse, err := http.DefaultClient.Do(reqDryRunReuse)
	if err != nil {
		t.Fatalf("request replay dry-run idempotency reuse: %v", err)
	}
	if respDryRunReuse.StatusCode != http.StatusOK {
		_ = respDryRunReuse.Body.Close()
		t.Fatalf("expected 200 for idempotency reuse, got %d", respDryRunReuse.StatusCode)
	}
	dryRunReused := readReplayJobEnvelope(respDryRunReuse)
	if !dryRunReused.IdempotencyReused || dryRunReused.Job.JobID != dryRunAccepted.Job.JobID {
		t.Fatalf("expected reused dry-run job, got %+v", dryRunReused)
	}
	reqDryRunConflict, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?dry_run=false&limit=11", nil)
	reqDryRunConflict.Header.Set("X-Admin-Token", "test-admin-token")
	reqDryRunConflict.Header.Set("X-Request-ID", "replay-dry-conflict-1")
	reqDryRunConflict.Header.Set("Idempotency-Key", "idem-replay-1")
	respDryRunConflict, err := http.DefaultClient.Do(reqDryRunConflict)
	if err != nil {
		t.Fatalf("request replay dry-run idempotency conflict: %v", err)
	}
	if respDryRunConflict.StatusCode != http.StatusConflict {
		_ = respDryRunConflict.Body.Close()
		t.Fatalf("expected 409 for idempotency conflict, got %d", respDryRunConflict.StatusCode)
	}
	dryRunConflict := readAdminError(respDryRunConflict)
	if dryRunConflict.RequestID != "replay-dry-conflict-1" || !strings.Contains(dryRunConflict.Error, "idempotency key") {
		t.Fatalf("unexpected idempotency conflict payload: %+v", dryRunConflict)
	}
	dryRunFinished := waitReplayJob("test-admin-token", "replay-status-dry-1", dryRunAccepted.Job.JobID)
	if dryRunFinished.Status != adminReplayJobStatusSucceeded || !dryRunFinished.DryRun {
		t.Fatalf("unexpected dry-run replay job completion: %+v", dryRunFinished)
	}

	reqCancelCompleted, _ := http.NewRequest(http.MethodDelete, replayBaseURL+"/"+dryRunAccepted.Job.JobID, nil)
	reqCancelCompleted.Header.Set("X-Admin-Token", "test-admin-token")
	reqCancelCompleted.Header.Set("X-Request-ID", "replay-cancel-completed-1")
	respCancelCompleted, err := http.DefaultClient.Do(reqCancelCompleted)
	if err != nil {
		t.Fatalf("request replay cancel completed job: %v", err)
	}
	if respCancelCompleted.StatusCode != http.StatusConflict {
		_ = respCancelCompleted.Body.Close()
		t.Fatalf("expected 409 for completed replay cancel, got %d", respCancelCompleted.StatusCode)
	}
	cancelCompletedErr := readAdminError(respCancelCompleted)
	if cancelCompletedErr.RequestID != "replay-cancel-completed-1" || !strings.Contains(cancelCompletedErr.Error, "cannot be cancelled") {
		t.Fatalf("unexpected replay cancel completed payload: %+v", cancelCompletedErr)
	}

	reqCancelMissing, _ := http.NewRequest(http.MethodDelete, replayBaseURL+"/missing-cancel-job", nil)
	reqCancelMissing.Header.Set("X-Admin-Token", "test-admin-token")
	reqCancelMissing.Header.Set("X-Request-ID", "replay-cancel-missing-1")
	respCancelMissing, err := http.DefaultClient.Do(reqCancelMissing)
	if err != nil {
		t.Fatalf("request replay cancel missing job: %v", err)
	}
	if respCancelMissing.StatusCode != http.StatusNotFound {
		_ = respCancelMissing.Body.Close()
		t.Fatalf("expected 404 for missing replay cancel, got %d", respCancelMissing.StatusCode)
	}
	cancelMissingErr := readAdminError(respCancelMissing)
	if cancelMissingErr.RequestID != "replay-cancel-missing-1" || !strings.Contains(cancelMissingErr.Error, "not found") {
		t.Fatalf("unexpected replay cancel missing payload: %+v", cancelMissingErr)
	}

	reqListInvalid, _ := http.NewRequest(http.MethodGet, replayBaseURL+"?status=unknown", nil)
	reqListInvalid.Header.Set("X-Admin-Token", "test-admin-token")
	reqListInvalid.Header.Set("X-Request-ID", "replay-list-invalid-1")
	respListInvalid, err := http.DefaultClient.Do(reqListInvalid)
	if err != nil {
		t.Fatalf("request replay list invalid status: %v", err)
	}
	if respListInvalid.StatusCode != http.StatusBadRequest {
		_ = respListInvalid.Body.Close()
		t.Fatalf("expected 400 for invalid replay list status, got %d", respListInvalid.StatusCode)
	}
	listInvalidErr := readAdminError(respListInvalid)
	if listInvalidErr.RequestID != "replay-list-invalid-1" || !strings.Contains(listInvalidErr.Error, "invalid status") {
		t.Fatalf("unexpected invalid replay list status payload: %+v", listInvalidErr)
	}

	reqListBadCursor, _ := http.NewRequest(http.MethodGet, replayBaseURL+"?cursor=a", nil)
	reqListBadCursor.Header.Set("X-Admin-Token", "test-admin-token")
	reqListBadCursor.Header.Set("X-Request-ID", "replay-list-cursor-1")
	respListBadCursor, err := http.DefaultClient.Do(reqListBadCursor)
	if err != nil {
		t.Fatalf("request replay list invalid cursor: %v", err)
	}
	if respListBadCursor.StatusCode != http.StatusBadRequest {
		_ = respListBadCursor.Body.Close()
		t.Fatalf("expected 400 for invalid replay list cursor, got %d", respListBadCursor.StatusCode)
	}
	listBadCursorErr := readAdminError(respListBadCursor)
	if listBadCursorErr.RequestID != "replay-list-cursor-1" || !strings.Contains(listBadCursorErr.Error, "invalid cursor") {
		t.Fatalf("unexpected invalid replay list cursor payload: %+v", listBadCursorErr)
	}

	reqListSucceeded, _ := http.NewRequest(http.MethodGet, replayBaseURL+"?status=succeeded&limit=1", nil)
	reqListSucceeded.Header.Set("X-Admin-Token", "test-admin-token")
	reqListSucceeded.Header.Set("X-Request-ID", "replay-list-success-1")
	respListSucceeded, err := http.DefaultClient.Do(reqListSucceeded)
	if err != nil {
		t.Fatalf("request replay list succeeded: %v", err)
	}
	if respListSucceeded.StatusCode != http.StatusOK {
		_ = respListSucceeded.Body.Close()
		t.Fatalf("expected 200 for replay list, got %d", respListSucceeded.StatusCode)
	}
	var listSucceeded struct {
		RequestID  string              `json:"request_id"`
		Status     string              `json:"status"`
		Limit      int                 `json:"limit"`
		Cursor     string              `json:"cursor"`
		NextCursor string              `json:"next_cursor"`
		Count      int                 `json:"count"`
		Summary    map[string]int      `json:"summary"`
		Jobs       []replayJobSnapshot `json:"jobs"`
	}
	listBody, err := io.ReadAll(respListSucceeded.Body)
	_ = respListSucceeded.Body.Close()
	if err != nil {
		t.Fatalf("read replay list body: %v", err)
	}
	if err := json.Unmarshal(listBody, &listSucceeded); err != nil {
		t.Fatalf("decode replay list body: %v body=%s", err, string(listBody))
	}
	if listSucceeded.RequestID != "replay-list-success-1" {
		t.Fatalf("unexpected replay list request id: %+v", listSucceeded)
	}
	if listSucceeded.Status != "succeeded" || listSucceeded.Limit != 1 {
		t.Fatalf("unexpected replay list filters: %+v", listSucceeded)
	}
	if listSucceeded.Cursor != "" {
		t.Fatalf("expected first replay list page to have empty cursor, got %q", listSucceeded.Cursor)
	}
	if listSucceeded.Count < 1 || len(listSucceeded.Jobs) < 1 {
		t.Fatalf("expected replay list to return succeeded jobs, got %+v", listSucceeded)
	}
	if listSucceeded.Summary[adminReplayJobStatusSucceeded] < 1 {
		t.Fatalf("expected succeeded summary count in replay list, got %+v", listSucceeded.Summary)
	}
	for _, listed := range listSucceeded.Jobs {
		if listed.Status != adminReplayJobStatusSucceeded {
			t.Fatalf("expected filtered replay list status=succeeded, got %+v", listed)
		}
	}
	if strings.TrimSpace(listSucceeded.NextCursor) == "" {
		t.Fatalf("expected replay list first page to include next_cursor, got %+v", listSucceeded)
	}

	reqListSucceededNext, _ := http.NewRequest(http.MethodGet, replayBaseURL+"?status=succeeded&limit=1&cursor="+listSucceeded.NextCursor, nil)
	reqListSucceededNext.Header.Set("X-Admin-Token", "test-admin-token")
	reqListSucceededNext.Header.Set("X-Request-ID", "replay-list-success-2")
	respListSucceededNext, err := http.DefaultClient.Do(reqListSucceededNext)
	if err != nil {
		t.Fatalf("request replay list second page: %v", err)
	}
	if respListSucceededNext.StatusCode != http.StatusOK {
		_ = respListSucceededNext.Body.Close()
		t.Fatalf("expected 200 for replay list second page, got %d", respListSucceededNext.StatusCode)
	}
	var listSucceededNext struct {
		RequestID  string              `json:"request_id"`
		Status     string              `json:"status"`
		Limit      int                 `json:"limit"`
		Cursor     string              `json:"cursor"`
		NextCursor string              `json:"next_cursor"`
		Count      int                 `json:"count"`
		Summary    map[string]int      `json:"summary"`
		Jobs       []replayJobSnapshot `json:"jobs"`
	}
	listNextBody, err := io.ReadAll(respListSucceededNext.Body)
	_ = respListSucceededNext.Body.Close()
	if err != nil {
		t.Fatalf("read replay list second body: %v", err)
	}
	if err := json.Unmarshal(listNextBody, &listSucceededNext); err != nil {
		t.Fatalf("decode replay list second body: %v body=%s", err, string(listNextBody))
	}
	if listSucceededNext.RequestID != "replay-list-success-2" || listSucceededNext.Cursor != listSucceeded.NextCursor {
		t.Fatalf("unexpected replay list second page payload: %+v", listSucceededNext)
	}
	if len(listSucceededNext.Jobs) < 1 {
		t.Fatalf("expected second replay list page to include jobs, got %+v", listSucceededNext)
	}
	if listSucceededNext.Jobs[0].JobID == listSucceeded.Jobs[0].JobID {
		t.Fatalf("expected second replay list page to advance cursor; got same job id %q", listSucceeded.Jobs[0].JobID)
	}

	reqMissingStatus, _ := http.NewRequest(http.MethodGet, replayBaseURL+"/missing-job-id", nil)
	reqMissingStatus.Header.Set("X-Admin-Token", "test-admin-token")
	reqMissingStatus.Header.Set("X-Request-ID", "replay-status-missing-1")
	respMissingStatus, err := http.DefaultClient.Do(reqMissingStatus)
	if err != nil {
		t.Fatalf("request replay missing status: %v", err)
	}
	if respMissingStatus.StatusCode != http.StatusNotFound {
		_ = respMissingStatus.Body.Close()
		t.Fatalf("expected 404 for missing replay job, got %d", respMissingStatus.StatusCode)
	}
	missingStatusErr := readAdminError(respMissingStatus)
	if missingStatusErr.RequestID != "replay-status-missing-1" || !strings.Contains(missingStatusErr.Error, "not found") {
		t.Fatalf("unexpected missing replay status payload: %+v", missingStatusErr)
	}
	logEntries := parseJSONLogEntries(t, logBuf.String())
	if _, ok := findLogEntry(logEntries, "admin request unauthorized", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okIP && okUA && okDuration &&
			path == "/admin/replay-dlq" &&
			method == http.MethodPost &&
			requestID == "replay-unauth-1" &&
			ip == "203.0.113.50" &&
			userAgent == "tap-admin-test/unauth" &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected unauthorized replay audit log with request metadata; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin replay dlq rejected", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		rawLimit, okLimit := entry["requested_limit_raw"].(string)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okIP && okUA && okLimit && okDuration &&
			path == "/admin/replay-dlq" &&
			method == http.MethodPost &&
			requestID == "replay-invalid-1" &&
			ip == "203.0.113.51" &&
			userAgent == "tap-admin-test/invalid" &&
			rawLimit == "invalid" &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected rejected replay audit log with request metadata; logs=%s", logBuf.String())
	}
	replayLog, ok := findLogEntry(logEntries, "admin replay dlq accepted", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		requested, okRequested := logFieldInt(entry, "requested_limit")
		effective, okEffective := logFieldInt(entry, "effective_limit")
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		dryRun, okDryRun := entry["dry_run"].(bool)
		capped, okCapped := entry["capped"].(bool)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okRequested && okEffective && okIP && okUA && okDryRun && okCapped && okDuration &&
			path == "/admin/replay-dlq" &&
			method == http.MethodPost &&
			requestID == "replay-success-1" &&
			requested == 1 &&
			effective == 1 &&
			ip == "203.0.113.52" &&
			userAgent == "tap-admin-test/success" &&
			!dryRun &&
			!capped &&
			durationMS >= 0
	})
	if !ok {
		t.Fatalf("expected replay acceptance audit log with requester ip/effective limit; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin replay dlq accepted", func(entry map[string]any) bool {
		requestID, okRequestID := entry["request_id"].(string)
		effective, okEffective := logFieldInt(entry, "effective_limit")
		capped, okCapped := entry["capped"].(bool)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		tokenSlot, okTokenSlot := entry["token_slot"].(string)
		return okRequestID && okEffective && okCapped && okDuration && okTokenSlot &&
			requestID == "replay-cap-1" &&
			effective == replayMaxLimit &&
			tokenSlot == "secondary" &&
			capped &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected capped replay acceptance audit log; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin replay dlq idempotency reused", func(entry map[string]any) bool {
		requestID, okRequestID := entry["request_id"].(string)
		return okRequestID && requestID == "replay-dry-2"
	}); !ok {
		t.Fatalf("expected replay idempotency reuse audit log; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin replay dlq idempotency conflict", func(entry map[string]any) bool {
		requestID, okRequestID := entry["request_id"].(string)
		return okRequestID && requestID == "replay-dry-conflict-1"
	}); !ok {
		t.Fatalf("expected replay idempotency conflict audit log; logs=%s", logBuf.String())
	}
	if _, exists := replayLog["requester_ip"]; !exists {
		t.Fatalf("expected replay audit log to include requester_ip")
	}
	metricsText := fetchMetricsBody(t, "http://127.0.0.1:"+intToString(port)+"/metrics")
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeUnauthorized,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeBadRequest,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeSuccess,
	}, 3)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeConflict,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayStatus,
		"outcome":  adminOutcomeSuccess,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayStatus,
		"outcome":  adminOutcomeNotFound,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQList,
		"outcome":  adminOutcomeBadRequest,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQList,
		"outcome":  adminOutcomeSuccess,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayCancel,
		"outcome":  adminOutcomeConflict,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayCancel,
		"outcome":  adminOutcomeNotFound,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_request_duration_seconds_count", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeSuccess,
	}, 3)
	assertMetricAtLeast(t, metricsText, "tap_admin_replay_jobs_total", map[string]string{
		"stage": "accepted",
	}, 3)
	assertMetricAtLeast(t, metricsText, "tap_admin_replay_jobs_total", map[string]string{
		"stage": "reused",
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_replay_jobs_total", map[string]string{
		"stage": "conflict",
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_replay_jobs_total", map[string]string{
		"stage": "succeeded",
	}, 3)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminPollerStatusEndpoint(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)

	notionAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/search" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"has_more":false,"next_cursor":""}`))
	}))
	defer notionAPI.Close()

	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"notion": {
				Mode:                "poll",
				BaseURL:             notionAPI.URL,
				AccessToken:         "notion-token",
				TenantID:            "tenant-ops",
				PollInterval:        25 * time.Millisecond,
				PollRateLimitPerSec: 9.0,
				PollBurst:           3,
			},
		},
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_POLLER_STATUS",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                port,
			BasePath:            "/webhooks",
			MaxBodySize:         1 << 20,
			AdminToken:          "test-admin-token",
			AdminTokenSecondary: "next-admin-token-status",
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logBuf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, testLogger)
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	statusURL := "http://127.0.0.1:" + intToString(port) + "/admin/poller-status"
	reqNoToken, _ := http.NewRequest(http.MethodGet, statusURL, nil)
	reqNoToken.Header.Set("X-Request-ID", "status-unauth-1")
	reqNoToken.Header.Set("X-Forwarded-For", "203.0.113.60")
	reqNoToken.Header.Set("User-Agent", "tap-status-test/unauth")
	respNoToken, err := http.DefaultClient.Do(reqNoToken)
	if err != nil {
		t.Fatalf("request poller status without token: %v", err)
	}
	if got := strings.TrimSpace(respNoToken.Header.Get("X-Request-ID")); got != "status-unauth-1" {
		t.Fatalf("expected unauthorized status response to echo request id, got %q", got)
	}
	if got := strings.TrimSpace(respNoToken.Header.Get("Content-Type")); !strings.Contains(got, "application/json") {
		t.Fatalf("expected unauthorized status response content-type application/json, got %q", got)
	}
	if respNoToken.StatusCode != http.StatusUnauthorized {
		_ = respNoToken.Body.Close()
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
	}
	var statusErr struct {
		RequestID string `json:"request_id"`
		Error     string `json:"error"`
	}
	statusErrBody, err := io.ReadAll(respNoToken.Body)
	_ = respNoToken.Body.Close()
	if err != nil {
		t.Fatalf("read unauthorized status response body: %v", err)
	}
	if err := json.Unmarshal(statusErrBody, &statusErr); err != nil {
		t.Fatalf("decode unauthorized status response body: %v body=%s", err, string(statusErrBody))
	}
	if statusErr.RequestID != "status-unauth-1" || statusErr.Error != "unauthorized" {
		t.Fatalf("unexpected unauthorized status response payload: %+v", statusErr)
	}

	type pollerStatus struct {
		Provider        string    `json:"provider"`
		TenantID        string    `json:"tenant_id"`
		Interval        string    `json:"interval"`
		RateLimitPerSec float64   `json:"rate_limit_per_sec"`
		Burst           int       `json:"burst"`
		FailureBudget   int       `json:"failure_budget"`
		CircuitBreak    string    `json:"circuit_break_duration"`
		JitterRatio     float64   `json:"jitter_ratio"`
		LastRunAt       time.Time `json:"last_run_at"`
		LastSuccessAt   time.Time `json:"last_success_at"`
		LastError       string    `json:"last_error"`
	}
	type pollerStatusResponse struct {
		RequestID string         `json:"request_id"`
		Provider  string         `json:"provider"`
		Tenant    string         `json:"tenant"`
		Count     int            `json:"count"`
		Pollers   []pollerStatus `json:"pollers"`
	}
	readStatus := func(url, token, reqID, userAgent, forwardedFor string) pollerStatusResponse {
		t.Helper()
		reqWithToken, _ := http.NewRequest(http.MethodGet, url, nil)
		reqWithToken.Header.Set("X-Admin-Token", token)
		if strings.TrimSpace(reqID) != "" {
			reqWithToken.Header.Set("X-Request-ID", reqID)
		}
		if strings.TrimSpace(userAgent) != "" {
			reqWithToken.Header.Set("User-Agent", userAgent)
		}
		if strings.TrimSpace(forwardedFor) != "" {
			reqWithToken.Header.Set("X-Forwarded-For", forwardedFor)
		}
		respWithToken, err := http.DefaultClient.Do(reqWithToken)
		if err != nil {
			t.Fatalf("request poller status with token: %v", err)
		}
		wantReqID := strings.TrimSpace(reqID)
		gotReqID := strings.TrimSpace(respWithToken.Header.Get("X-Request-ID"))
		if wantReqID != "" && gotReqID != wantReqID {
			_ = respWithToken.Body.Close()
			t.Fatalf("expected status response request id %q, got %q", wantReqID, gotReqID)
		}
		if wantReqID == "" && gotReqID == "" {
			_ = respWithToken.Body.Close()
			t.Fatalf("expected status response to include generated request id")
		}
		if respWithToken.StatusCode != http.StatusOK {
			_ = respWithToken.Body.Close()
			t.Fatalf("expected 200 with admin token, got %d", respWithToken.StatusCode)
		}
		body, err := io.ReadAll(respWithToken.Body)
		_ = respWithToken.Body.Close()
		if err != nil {
			t.Fatalf("read status response body: %v", err)
		}
		var out pollerStatusResponse
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode status response body: %v", err)
		}
		return out
	}

	var got pollerStatusResponse
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got = readStatus(statusURL, "test-admin-token", "", "", "")
		if len(got.Pollers) > 0 && !got.Pollers[0].LastRunAt.IsZero() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if got.Count != 1 || len(got.Pollers) != 1 {
		t.Fatalf("expected one poller status, got count=%d len=%d", got.Count, len(got.Pollers))
	}
	status := got.Pollers[0]
	if status.Provider != "notion" || status.TenantID != "tenant-ops" {
		t.Fatalf("unexpected poller identity: %+v", status)
	}
	if status.Interval != "25ms" || status.RateLimitPerSec != 9.0 || status.Burst != 3 {
		t.Fatalf("unexpected poller config status: %+v", status)
	}
	if status.FailureBudget != 5 || status.CircuitBreak != "30s" || status.JitterRatio != 0 {
		t.Fatalf("unexpected poller resilience status: %+v", status)
	}
	if status.LastRunAt.IsZero() || status.LastSuccessAt.IsZero() {
		t.Fatalf("expected poller to have run successfully, got %+v", status)
	}
	if status.LastError != "" {
		t.Fatalf("expected no poller error, got %q", status.LastError)
	}

	filteredProvider := readStatus(statusURL+"?provider=NOTION", "next-admin-token-status", "status-provider-1", "tap-status-test/provider", "203.0.113.61")
	if filteredProvider.Provider != "NOTION" || filteredProvider.Count != 1 || len(filteredProvider.Pollers) != 1 {
		t.Fatalf("unexpected provider-filter response: %+v", filteredProvider)
	}
	if filteredProvider.RequestID != "status-provider-1" {
		t.Fatalf("expected provider-filter request_id in response, got %q", filteredProvider.RequestID)
	}
	filteredTenant := readStatus(statusURL+"?tenant=missing-tenant", "test-admin-token", "status-tenant-1", "tap-status-test/tenant", "203.0.113.62")
	if filteredTenant.Tenant != "missing-tenant" || filteredTenant.Count != 0 || len(filteredTenant.Pollers) != 0 {
		t.Fatalf("unexpected tenant-filter response: %+v", filteredTenant)
	}
	if filteredTenant.RequestID != "status-tenant-1" {
		t.Fatalf("expected tenant-filter request_id in response, got %q", filteredTenant.RequestID)
	}
	filteredCombo := readStatus(statusURL+"?provider=notion&tenant=tenant-ops", "test-admin-token", "", "", "")
	if filteredCombo.Count != 1 || len(filteredCombo.Pollers) != 1 {
		t.Fatalf("unexpected combined-filter response: %+v", filteredCombo)
	}
	logEntries := parseJSONLogEntries(t, logBuf.String())
	if _, ok := findLogEntry(logEntries, "admin request unauthorized", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okIP && okUA && okDuration &&
			path == "/admin/poller-status" &&
			method == http.MethodGet &&
			requestID == "status-unauth-1" &&
			ip == "203.0.113.60" &&
			userAgent == "tap-status-test/unauth" &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected unauthorized poller status audit log with request metadata; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin poller status fetched", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		tokenSlot, okTokenSlot := entry["token_slot"].(string)
		provider, okProvider := entry["provider_filter"].(string)
		tenant, okTenant := entry["tenant_filter"].(string)
		count, okCount := logFieldInt(entry, "poller_count")
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okTokenSlot && okProvider && okTenant && okCount && okIP && okUA && okDuration &&
			path == "/admin/poller-status" &&
			method == http.MethodGet &&
			requestID == "status-provider-1" &&
			tokenSlot == "secondary" &&
			provider == "NOTION" &&
			tenant == "" &&
			count == 1 &&
			ip == "203.0.113.61" &&
			userAgent == "tap-status-test/provider" &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected provider-filter poller status audit log with requester ip; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin poller status fetched", func(entry map[string]any) bool {
		requestID, okRequestID := entry["request_id"].(string)
		tenant, okTenant := entry["tenant_filter"].(string)
		count, okCount := logFieldInt(entry, "poller_count")
		userAgent, okUA := entry["user_agent"].(string)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okRequestID && okTenant && okCount && okUA && okDuration &&
			requestID == "status-tenant-1" &&
			tenant == "missing-tenant" &&
			count == 0 &&
			userAgent == "tap-status-test/tenant" &&
			durationMS >= 0
	}); !ok {
		t.Fatalf("expected tenant-filter poller status audit log with poller_count=0; logs=%s", logBuf.String())
	}
	metricsText := fetchMetricsBody(t, "http://127.0.0.1:"+intToString(port)+"/metrics")
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointPollerStatus,
		"outcome":  adminOutcomeUnauthorized,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointPollerStatus,
		"outcome":  adminOutcomeSuccess,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_request_duration_seconds_count", map[string]string{
		"endpoint": adminEndpointPollerStatus,
		"outcome":  adminOutcomeSuccess,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_poller_consecutive_failures", map[string]string{
		"provider": "notion",
		"tenant":   "tenant-ops",
	}, 0)
	assertMetricAtLeast(t, metricsText, "tap_poller_stuck", map[string]string{
		"provider": "notion",
		"tenant":   "tenant-ops",
	}, 0)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminReplayEndpointAllowlistAndMTLS(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_ADMIN_MTLS",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                      port,
			BasePath:                  "/webhooks",
			MaxBodySize:               1 << 20,
			AdminToken:                "test-admin-token",
			AdminAllowedCIDRs:         []string{"203.0.113.0/24"},
			AdminMTLSRequired:         true,
			AdminMTLSClientCertHeader: "X-Client-Cert",
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}
	replayURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq?dry_run=true&limit=1"

	readErr := func(resp *http.Response) string {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read admin error body: %v", err)
		}
		var out struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode admin error body: %v body=%s", err, string(body))
		}
		return out.Error
	}

	reqCIDRBlocked, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqCIDRBlocked.Header.Set("X-Request-ID", "replay-cidr-1")
	reqCIDRBlocked.Header.Set("X-Admin-Token", "test-admin-token")
	reqCIDRBlocked.Header.Set("X-Forwarded-For", "198.51.100.20")
	respCIDRBlocked, err := http.DefaultClient.Do(reqCIDRBlocked)
	if err != nil {
		t.Fatalf("request replay from blocked cidr: %v", err)
	}
	if respCIDRBlocked.StatusCode != http.StatusForbidden {
		_ = respCIDRBlocked.Body.Close()
		t.Fatalf("expected blocked cidr status 403, got %d", respCIDRBlocked.StatusCode)
	}
	if got := readErr(respCIDRBlocked); got != "forbidden" {
		t.Fatalf("unexpected blocked cidr error %q", got)
	}

	reqMTLSMissing, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqMTLSMissing.Header.Set("X-Request-ID", "replay-mtls-1")
	reqMTLSMissing.Header.Set("X-Admin-Token", "test-admin-token")
	reqMTLSMissing.Header.Set("X-Forwarded-For", "203.0.113.22")
	respMTLSMissing, err := http.DefaultClient.Do(reqMTLSMissing)
	if err != nil {
		t.Fatalf("request replay with missing mTLS: %v", err)
	}
	if respMTLSMissing.StatusCode != http.StatusForbidden {
		_ = respMTLSMissing.Body.Close()
		t.Fatalf("expected missing mTLS status 403, got %d", respMTLSMissing.StatusCode)
	}
	if got := readErr(respMTLSMissing); !strings.Contains(strings.ToLower(got), "mTLS") && !strings.Contains(strings.ToLower(got), "certificate") {
		t.Fatalf("unexpected missing mTLS error %q", got)
	}

	reqTokenMissing, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqTokenMissing.Header.Set("X-Request-ID", "replay-auth-1")
	reqTokenMissing.Header.Set("X-Forwarded-For", "203.0.113.23")
	reqTokenMissing.Header.Set("X-Client-Cert", "subject=client-a")
	respTokenMissing, err := http.DefaultClient.Do(reqTokenMissing)
	if err != nil {
		t.Fatalf("request replay with missing token: %v", err)
	}
	if respTokenMissing.StatusCode != http.StatusUnauthorized {
		_ = respTokenMissing.Body.Close()
		t.Fatalf("expected missing token status 401, got %d", respTokenMissing.StatusCode)
	}

	reqAllowed, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqAllowed.Header.Set("X-Request-ID", "replay-ok-1")
	reqAllowed.Header.Set("X-Forwarded-For", "203.0.113.24")
	reqAllowed.Header.Set("X-Client-Cert", "subject=client-a")
	reqAllowed.Header.Set("X-Admin-Token", "test-admin-token")
	respAllowed, err := http.DefaultClient.Do(reqAllowed)
	if err != nil {
		t.Fatalf("request replay with allowlist+mTLS+token: %v", err)
	}
	if respAllowed.StatusCode != http.StatusAccepted {
		_ = respAllowed.Body.Close()
		t.Fatalf("expected accepted status 202, got %d", respAllowed.StatusCode)
	}
	_, _ = io.Copy(io.Discard, respAllowed.Body)
	_ = respAllowed.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminEndpointsRoleScopedTokens(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)

	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_ADMIN_ROLE_SCOPES",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                     port,
			BasePath:                 "/webhooks",
			MaxBodySize:              1 << 20,
			AdminTokenRead:           "read-admin-token",
			AdminTokenReplay:         "replay-admin-token",
			AdminTokenCancel:         "cancel-admin-token",
			AdminReplayRequireReason: true,
			AdminReplayReasonMinLen:  8,
			AdminRateLimitPerSec:     100,
			AdminRateLimitBurst:      100,
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	replayURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq?dry_run=true&limit=1"

	reqReplayReadToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqReplayReadToken.Header.Set("X-Admin-Token", "read-admin-token")
	reqReplayReadToken.Header.Set("X-Request-ID", "role-replay-read-1")
	reqReplayReadToken.Header.Set("X-Admin-Reason", "validate replay scope")
	respReplayReadToken, err := http.DefaultClient.Do(reqReplayReadToken)
	if err != nil {
		t.Fatalf("request replay with read token: %v", err)
	}
	if respReplayReadToken.StatusCode != http.StatusUnauthorized {
		_ = respReplayReadToken.Body.Close()
		t.Fatalf("expected replay with read token to be unauthorized, got %d", respReplayReadToken.StatusCode)
	}
	_ = respReplayReadToken.Body.Close()

	reqReplayMissingReason, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqReplayMissingReason.Header.Set("X-Admin-Token", "replay-admin-token")
	reqReplayMissingReason.Header.Set("X-Request-ID", "role-replay-reason-1")
	respReplayMissingReason, err := http.DefaultClient.Do(reqReplayMissingReason)
	if err != nil {
		t.Fatalf("request replay missing reason: %v", err)
	}
	if respReplayMissingReason.StatusCode != http.StatusBadRequest {
		_ = respReplayMissingReason.Body.Close()
		t.Fatalf("expected replay missing reason to be 400, got %d", respReplayMissingReason.StatusCode)
	}
	_ = respReplayMissingReason.Body.Close()

	reqReplayOK, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqReplayOK.Header.Set("X-Admin-Token", "replay-admin-token")
	reqReplayOK.Header.Set("X-Request-ID", "role-replay-ok-1")
	reqReplayOK.Header.Set("X-Admin-Reason", "manually verify role-scoped replay")
	respReplayOK, err := http.DefaultClient.Do(reqReplayOK)
	if err != nil {
		t.Fatalf("request replay with replay token: %v", err)
	}
	if respReplayOK.StatusCode != http.StatusAccepted {
		_ = respReplayOK.Body.Close()
		t.Fatalf("expected replay with replay token to be accepted, got %d", respReplayOK.StatusCode)
	}
	var replayAccepted struct {
		Job struct {
			JobID string `json:"job_id"`
		} `json:"job"`
	}
	replayAcceptedBody, err := io.ReadAll(respReplayOK.Body)
	_ = respReplayOK.Body.Close()
	if err != nil {
		t.Fatalf("read replay accepted body: %v", err)
	}
	if err := json.Unmarshal(replayAcceptedBody, &replayAccepted); err != nil {
		t.Fatalf("decode replay accepted body: %v body=%s", err, string(replayAcceptedBody))
	}
	if replayAccepted.Job.JobID == "" {
		t.Fatalf("expected replay job id in accepted payload")
	}

	statusURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq/" + replayAccepted.Job.JobID
	reqStatusRead, _ := http.NewRequest(http.MethodGet, statusURL, nil)
	reqStatusRead.Header.Set("X-Admin-Token", "read-admin-token")
	reqStatusRead.Header.Set("X-Request-ID", "role-status-read-1")
	respStatusRead, err := http.DefaultClient.Do(reqStatusRead)
	if err != nil {
		t.Fatalf("request replay status with read token: %v", err)
	}
	if respStatusRead.StatusCode != http.StatusOK {
		_ = respStatusRead.Body.Close()
		t.Fatalf("expected replay status with read token 200, got %d", respStatusRead.StatusCode)
	}
	_ = respStatusRead.Body.Close()

	reqCancelMissingReason, _ := http.NewRequest(http.MethodDelete, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq/missing-role-job", nil)
	reqCancelMissingReason.Header.Set("X-Admin-Token", "cancel-admin-token")
	reqCancelMissingReason.Header.Set("X-Request-ID", "role-cancel-reason-1")
	respCancelMissingReason, err := http.DefaultClient.Do(reqCancelMissingReason)
	if err != nil {
		t.Fatalf("request cancel missing reason: %v", err)
	}
	if respCancelMissingReason.StatusCode != http.StatusBadRequest {
		_ = respCancelMissingReason.Body.Close()
		t.Fatalf("expected cancel missing reason to be 400, got %d", respCancelMissingReason.StatusCode)
	}
	_ = respCancelMissingReason.Body.Close()

	reqCancelMissing, _ := http.NewRequest(http.MethodDelete, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq/missing-role-job", nil)
	reqCancelMissing.Header.Set("X-Admin-Token", "cancel-admin-token")
	reqCancelMissing.Header.Set("X-Request-ID", "role-cancel-missing-1")
	reqCancelMissing.Header.Set("X-Admin-Reason", "cleanup unknown replay id")
	respCancelMissing, err := http.DefaultClient.Do(reqCancelMissing)
	if err != nil {
		t.Fatalf("request cancel with cancel token: %v", err)
	}
	if respCancelMissing.StatusCode != http.StatusNotFound {
		_ = respCancelMissing.Body.Close()
		t.Fatalf("expected cancel missing job to be 404, got %d", respCancelMissing.StatusCode)
	}
	_ = respCancelMissing.Body.Close()

	reqCancelReplayToken, _ := http.NewRequest(http.MethodDelete, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq/missing-role-job", nil)
	reqCancelReplayToken.Header.Set("X-Admin-Token", "replay-admin-token")
	reqCancelReplayToken.Header.Set("X-Request-ID", "role-cancel-replay-token-1")
	reqCancelReplayToken.Header.Set("X-Admin-Reason", "should fail")
	respCancelReplayToken, err := http.DefaultClient.Do(reqCancelReplayToken)
	if err != nil {
		t.Fatalf("request cancel with replay token: %v", err)
	}
	if respCancelReplayToken.StatusCode != http.StatusUnauthorized {
		_ = respCancelReplayToken.Body.Close()
		t.Fatalf("expected cancel with replay token to be unauthorized, got %d", respCancelReplayToken.StatusCode)
	}
	_ = respCancelReplayToken.Body.Close()

	reqListRead, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?status=succeeded&limit=5", nil)
	reqListRead.Header.Set("X-Admin-Token", "read-admin-token")
	reqListRead.Header.Set("X-Request-ID", "role-list-read-1")
	respListRead, err := http.DefaultClient.Do(reqListRead)
	if err != nil {
		t.Fatalf("request replay list with read token: %v", err)
	}
	if respListRead.StatusCode != http.StatusOK {
		_ = respListRead.Body.Close()
		t.Fatalf("expected replay list with read token 200, got %d", respListRead.StatusCode)
	}
	_ = respListRead.Body.Close()

	reqPollerRead, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/poller-status", nil)
	reqPollerRead.Header.Set("X-Admin-Token", "read-admin-token")
	reqPollerRead.Header.Set("X-Request-ID", "role-poller-read-1")
	respPollerRead, err := http.DefaultClient.Do(reqPollerRead)
	if err != nil {
		t.Fatalf("request poller status with read token: %v", err)
	}
	if respPollerRead.StatusCode != http.StatusOK {
		_ = respPollerRead.Body.Close()
		t.Fatalf("expected poller status with read token 200, got %d", respPollerRead.StatusCode)
	}
	_ = respPollerRead.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminReplayUnderContention(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_REPLAY_CONTENTION",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                     port,
			BasePath:                 "/webhooks",
			MaxBodySize:              1 << 20,
			AdminToken:               "test-admin-token",
			AdminRateLimitPerSec:     200,
			AdminRateLimitBurst:      200,
			AdminReplayMaxConcurrent: 1,
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	for i := 0; i < 10; i++ {
		rec := dlq.Record{
			Stage:           "publish",
			Provider:        "test",
			Reason:          "contention replay test",
			OriginalSubject: "siphon.tap.replay.contention.updated",
			OriginalDedupID: fmt.Sprintf("contention_%d", i),
			OriginalPayload: []byte(fmt.Sprintf(`{"id":"contention_%d"}`, i)),
		}
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal dlq record: %v", err)
		}
		msg := &nats.Msg{
			Subject: "siphon.dlq.publish.test",
			Data:    data,
			Header:  nats.Header{},
		}
		msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("dlq_contention_%d", i))
		if _, err := js.PublishMsg(msg); err != nil {
			t.Fatalf("publish dlq message %d: %v", i, err)
		}
	}

	type replayJobSnapshot struct {
		JobID    string `json:"job_id"`
		Status   string `json:"status"`
		Replayed int    `json:"replayed"`
		Error    string `json:"error"`
	}
	type replayJobEnvelope struct {
		Job replayJobSnapshot `json:"job"`
	}
	readReplayJob := func(resp *http.Response) replayJobEnvelope {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read replay response body: %v", err)
		}
		var out replayJobEnvelope
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode replay response body: %v body=%s", err, string(body))
		}
		return out
	}
	waitJob := func(jobID string) replayJobSnapshot {
		t.Helper()
		statusURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq/" + jobID
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			req, _ := http.NewRequest(http.MethodGet, statusURL, nil)
			req.Header.Set("X-Admin-Token", "test-admin-token")
			req.Header.Set("X-Forwarded-For", "203.0.113.88")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request replay status: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				_ = resp.Body.Close()
				t.Fatalf("expected replay status 200, got %d", resp.StatusCode)
			}
			out := readReplayJob(resp)
			if out.Job.Status == adminReplayJobStatusSucceeded || out.Job.Status == adminReplayJobStatusFailed {
				return out.Job
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for replay job %q", jobID)
		return replayJobSnapshot{}
	}

	jobIDs := make(chan string, 4)
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?limit=2", nil)
			req.Header.Set("X-Admin-Token", "test-admin-token")
			req.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", 90+i))
			req.Header.Set("X-Request-ID", fmt.Sprintf("replay-contention-%d", i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs <- err
				return
			}
			if resp.StatusCode != http.StatusAccepted {
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				errs <- fmt.Errorf("unexpected status %d body=%s", resp.StatusCode, string(body))
				return
			}
			out := readReplayJob(resp)
			if out.Job.JobID == "" {
				errs <- fmt.Errorf("missing job id in contention response")
				return
			}
			jobIDs <- out.Job.JobID
			errs <- nil
		}(i)
	}
	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("submit contention replay job: %v", err)
		}
	}

	totalReplayed := 0
	for i := 0; i < 4; i++ {
		jobID := <-jobIDs
		job := waitJob(jobID)
		if job.Status == adminReplayJobStatusSucceeded {
			totalReplayed += job.Replayed
		}
	}
	if totalReplayed == 0 {
		t.Fatalf("expected at least one replayed event across contention jobs")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminEndpointsRateLimited(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)

	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_ADMIN_RATELIMIT",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                 port,
			BasePath:             "/webhooks",
			MaxBodySize:          1 << 20,
			AdminToken:           "test-admin-token",
			AdminRateLimitPerSec: 0.1,
			AdminRateLimitBurst:  1,
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logBuf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, testLogger)
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	type adminErrorResponse struct {
		RequestID string `json:"request_id"`
		Error     string `json:"error"`
	}
	readAdminError := func(resp *http.Response) adminErrorResponse {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read admin error response body: %v", err)
		}
		var out adminErrorResponse
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode admin error response body: %v body=%s", err, string(body))
		}
		return out
	}

	replayURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq"
	reqReplayUnauth, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqReplayUnauth.Header.Set("X-Request-ID", "rate-replay-1")
	reqReplayUnauth.Header.Set("X-Forwarded-For", "203.0.113.71")
	respReplayUnauth, err := http.DefaultClient.Do(reqReplayUnauth)
	if err != nil {
		t.Fatalf("request replay unauthorized baseline: %v", err)
	}
	if respReplayUnauth.StatusCode != http.StatusUnauthorized {
		_ = respReplayUnauth.Body.Close()
		t.Fatalf("expected replay baseline status 401, got %d", respReplayUnauth.StatusCode)
	}
	unauthErr := readAdminError(respReplayUnauth)
	if unauthErr.RequestID != "rate-replay-1" || unauthErr.Error != "unauthorized" {
		t.Fatalf("unexpected replay baseline error payload: %+v", unauthErr)
	}

	reqReplayLimited, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqReplayLimited.Header.Set("X-Request-ID", "rate-replay-2")
	reqReplayLimited.Header.Set("X-Forwarded-For", "203.0.113.71")
	respReplayLimited, err := http.DefaultClient.Do(reqReplayLimited)
	if err != nil {
		t.Fatalf("request replay expected rate-limit: %v", err)
	}
	if respReplayLimited.StatusCode != http.StatusTooManyRequests {
		_ = respReplayLimited.Body.Close()
		t.Fatalf("expected replay rate-limited status 429, got %d", respReplayLimited.StatusCode)
	}
	if got := strings.TrimSpace(respReplayLimited.Header.Get("Retry-After")); got != "10" {
		_ = respReplayLimited.Body.Close()
		t.Fatalf("expected replay Retry-After=10, got %q", got)
	}
	replayLimitedErr := readAdminError(respReplayLimited)
	if replayLimitedErr.RequestID != "rate-replay-2" || replayLimitedErr.Error != "rate limit exceeded" {
		t.Fatalf("unexpected replay rate-limit payload: %+v", replayLimitedErr)
	}

	statusURL := "http://127.0.0.1:" + intToString(port) + "/admin/poller-status"
	reqStatusOK, _ := http.NewRequest(http.MethodGet, statusURL, nil)
	reqStatusOK.Header.Set("X-Admin-Token", "test-admin-token")
	reqStatusOK.Header.Set("X-Request-ID", "rate-status-1")
	reqStatusOK.Header.Set("X-Forwarded-For", "203.0.113.73")
	respStatusOK, err := http.DefaultClient.Do(reqStatusOK)
	if err != nil {
		t.Fatalf("request status baseline: %v", err)
	}
	if respStatusOK.StatusCode != http.StatusOK {
		_ = respStatusOK.Body.Close()
		t.Fatalf("expected status baseline 200, got %d", respStatusOK.StatusCode)
	}
	_, _ = io.Copy(io.Discard, respStatusOK.Body)
	_ = respStatusOK.Body.Close()

	reqStatusLimited, _ := http.NewRequest(http.MethodGet, statusURL, nil)
	reqStatusLimited.Header.Set("X-Admin-Token", "test-admin-token")
	reqStatusLimited.Header.Set("X-Request-ID", "rate-status-2")
	reqStatusLimited.Header.Set("X-Forwarded-For", "203.0.113.74")
	respStatusLimited, err := http.DefaultClient.Do(reqStatusLimited)
	if err != nil {
		t.Fatalf("request status expected rate-limit: %v", err)
	}
	if respStatusLimited.StatusCode != http.StatusTooManyRequests {
		_ = respStatusLimited.Body.Close()
		t.Fatalf("expected status rate-limited 429, got %d", respStatusLimited.StatusCode)
	}
	if got := strings.TrimSpace(respStatusLimited.Header.Get("Retry-After")); got != "10" {
		_ = respStatusLimited.Body.Close()
		t.Fatalf("expected status Retry-After=10, got %q", got)
	}
	statusLimitedErr := readAdminError(respStatusLimited)
	if statusLimitedErr.RequestID != "rate-status-2" || statusLimitedErr.Error != "rate limit exceeded" {
		t.Fatalf("unexpected status rate-limit payload: %+v", statusLimitedErr)
	}

	metricsText := fetchMetricsBody(t, "http://127.0.0.1:"+intToString(port)+"/metrics")
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeRateLimited,
	}, 1)
	assertMetricAtLeast(t, metricsText, "tap_admin_requests_total", map[string]string{
		"endpoint": adminEndpointPollerStatus,
		"outcome":  adminOutcomeRateLimited,
	}, 1)

	logEntries := parseJSONLogEntries(t, logBuf.String())
	if _, ok := findLogEntry(logEntries, "admin request rate limited", func(entry map[string]any) bool {
		endpoint, okEndpoint := entry["endpoint"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		retryAfter, okRetryAfter := logFieldInt(entry, "retry_after_seconds")
		return okEndpoint && okRequestID && okRetryAfter &&
			endpoint == adminEndpointReplayDLQ &&
			requestID == "rate-replay-2" &&
			retryAfter == 10
	}); !ok {
		t.Fatalf("expected replay rate-limited audit log; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin request rate limited", func(entry map[string]any) bool {
		endpoint, okEndpoint := entry["endpoint"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		retryAfter, okRetryAfter := logFieldInt(entry, "retry_after_seconds")
		return okEndpoint && okRequestID && okRetryAfter &&
			endpoint == adminEndpointPollerStatus &&
			requestID == "rate-status-2" &&
			retryAfter == 10
	}); !ok {
		t.Fatalf("expected status rate-limited audit log; logs=%s", logBuf.String())
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func parseJSONLogEntries(t *testing.T, raw string) []map[string]any {
	t.Helper()
	entries := make([]map[string]any, 0)
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry := map[string]any{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan logs: %v", err)
	}
	return entries
}

func findLogEntry(entries []map[string]any, message string, predicate func(entry map[string]any) bool) (map[string]any, bool) {
	for _, entry := range entries {
		msg, _ := entry["msg"].(string)
		if msg != message {
			continue
		}
		if predicate == nil || predicate(entry) {
			return entry, true
		}
	}
	return nil, false
}

func logFieldInt(entry map[string]any, key string) (int, bool) {
	value, ok := entry[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func fetchMetricsBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	return string(body)
}

func assertMetricAtLeast(t *testing.T, metricsText, metricName string, wantLabels map[string]string, min float64) {
	t.Helper()
	value, ok := metricValue(metricsText, metricName, wantLabels)
	if !ok {
		t.Fatalf("metric %s with labels %v not found", metricName, wantLabels)
	}
	if value < min {
		t.Fatalf("metric %s with labels %v expected >= %v, got %v", metricName, wantLabels, min, value)
	}
}

func metricValue(metricsText, metricName string, wantLabels map[string]string) (float64, bool) {
	scanner := bufio.NewScanner(strings.NewReader(metricsText))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		nameAndLabels, rawValue, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		name := nameAndLabels
		labels := map[string]string{}
		if open := strings.Index(nameAndLabels, "{"); open >= 0 {
			closeIdx := strings.LastIndex(nameAndLabels, "}")
			if closeIdx <= open {
				continue
			}
			name = nameAndLabels[:open]
			labelString := nameAndLabels[open+1 : closeIdx]
			for _, pair := range strings.Split(labelString, ",") {
				pair = strings.TrimSpace(pair)
				if pair == "" {
					continue
				}
				k, v, ok := strings.Cut(pair, "=")
				if !ok {
					continue
				}
				labels[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"")
			}
		}
		if name != metricName {
			continue
		}
		match := true
		for k, v := range wantLabels {
			if labels[k] != v {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
		if err != nil {
			continue
		}
		return value, true
	}
	return 0, false
}

func waitForStatusOrError(url string, want int, timeout time.Duration, errCh <-chan error) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			if err == nil {
				return context.Canceled
			}
			return err
		default:
		}
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func runNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server not ready")
	}
	t.Cleanup(func() {
		if s.Running() {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
	return s
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

type testReadiness struct {
	err error
}

func (r testReadiness) Ready() error {
	return r.err
}

func intToString(v int) string {
	return fmt.Sprintf("%d", v)
}

func signGeneric(body []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
