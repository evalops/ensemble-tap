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

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/dlq"
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
			Stream:        "ENSEMBLE_TAP_CMD_TEST",
			SubjectPrefix: "ensemble.tap",
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
			Stream:        "ENSEMBLE_TAP_CMD_TEST_READY",
			SubjectPrefix: "ensemble.tap",
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

func TestSecureTokenEqual(t *testing.T) {
	if secureTokenEqual("", "token") {
		t.Fatalf("expected empty actual token to fail")
	}
	if secureTokenEqual("token", "") {
		t.Fatalf("expected empty expected token to fail")
	}
	if !secureTokenEqual("token", "token") {
		t.Fatalf("expected matching tokens to pass")
	}
	if !secureTokenEqual(" token ", "token") {
		t.Fatalf("expected trimmed tokens to pass")
	}
	if secureTokenEqual("token-1", "token-2") {
		t.Fatalf("expected non-matching tokens to fail")
	}
}

func TestAuthorizeAdminToken(t *testing.T) {
	if ok, slot := authorizeAdminToken("token-a", "token-a", "token-b"); !ok || slot != "primary" {
		t.Fatalf("expected primary token match, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-b", "token-a", "token-b"); !ok || slot != "secondary" {
		t.Fatalf("expected secondary token match, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-c", "token-a", "token-b"); ok || slot != "" {
		t.Fatalf("expected unauthorized token, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-b", "token-a", ""); ok || slot != "" {
		t.Fatalf("expected no secondary match when secondary token not configured, got ok=%v slot=%q", ok, slot)
	}
}

func TestParseReplayDLQLimit(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantReq    int
		wantEff    int
		wantCapped bool
		wantErr    bool
		wantErrSub string
	}{
		{name: "default when empty", raw: "", wantReq: 0, wantEff: defaultReplayDLQLimit, wantCapped: false},
		{name: "custom valid", raw: "25", wantReq: 25, wantEff: 25, wantCapped: false},
		{name: "cap large limit", raw: "99999", wantReq: 99999, wantEff: maxReplayDLQLimit, wantCapped: true},
		{name: "invalid zero", raw: "0", wantErr: true, wantErrSub: "greater than 0"},
		{name: "invalid negative", raw: "-3", wantErr: true, wantErrSub: "greater than 0"},
		{name: "invalid text", raw: "abc", wantErr: true, wantErrSub: "positive integer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, eff, capped, err := parseReplayDLQLimit(tt.raw, maxReplayDLQLimit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.wantErrSub != "" && !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("unexpected error %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if req != tt.wantReq || eff != tt.wantEff || capped != tt.wantCapped {
				t.Fatalf("unexpected parse result: req=%d eff=%d capped=%v", req, eff, capped)
			}
		})
	}

	req, eff, capped, err := parseReplayDLQLimit("99999", 500)
	if err != nil {
		t.Fatalf("unexpected error with custom max limit: %v", err)
	}
	if req != 99999 || eff != 500 || !capped {
		t.Fatalf("expected custom max cap to apply, got req=%d eff=%d capped=%v", req, eff, capped)
	}

	req, eff, capped, err = parseReplayDLQLimit("", 50)
	if err != nil {
		t.Fatalf("unexpected error with empty limit and low max: %v", err)
	}
	if req != 0 || eff != 50 || !capped {
		t.Fatalf("expected empty limit to respect max cap, got req=%d eff=%d capped=%v", req, eff, capped)
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
			Stream:        "ENSEMBLE_TAP_CMD_TEST_REPLAY",
			SubjectPrefix: "ensemble.tap",
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
		_ = respNoToken.Body.Close()
		t.Fatalf("expected unauthorized response to echo request id, got %q", got)
	}
	_ = respNoToken.Body.Close()
	if respNoToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
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
		_ = respInvalidLimit.Body.Close()
		t.Fatalf("expected invalid-limit response to echo request id, got %q", got)
	}
	_ = respInvalidLimit.Body.Close()
	if respInvalidLimit.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid replay limit, got %d", respInvalidLimit.StatusCode)
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

	subject := "ensemble.tap.replay.test.updated"
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
		Subject: "ensemble.dlq.publish.test",
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
		t.Fatalf("expected success response to echo request id, got %q", got)
	}
	if respWithToken.StatusCode != http.StatusOK {
		_ = respWithToken.Body.Close()
		t.Fatalf("expected 200 with admin token, got %d", respWithToken.StatusCode)
	}
	type replayResponse struct {
		RequestID      string `json:"request_id"`
		Replayed       int    `json:"replayed"`
		RequestedLimit int    `json:"requested_limit"`
		EffectiveLimit int    `json:"effective_limit"`
		MaxLimit       int    `json:"max_limit"`
		Capped         bool   `json:"capped"`
	}
	var replayResult replayResponse
	replayBody, err := io.ReadAll(respWithToken.Body)
	_ = respWithToken.Body.Close()
	if err != nil {
		t.Fatalf("read replay response body: %v", err)
	}
	if err := json.Unmarshal(replayBody, &replayResult); err != nil {
		t.Fatalf("decode replay response body: %v", err)
	}
	if replayResult.Replayed != 1 ||
		replayResult.RequestID != "replay-success-1" ||
		replayResult.RequestedLimit != 1 ||
		replayResult.EffectiveLimit != 1 ||
		replayResult.MaxLimit != replayMaxLimit ||
		replayResult.Capped {
		t.Fatalf("unexpected replay response: %+v", replayResult)
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
		t.Fatalf("expected capped response to echo request id, got %q", got)
	}
	if respWithCap.StatusCode != http.StatusOK {
		_ = respWithCap.Body.Close()
		t.Fatalf("expected 200 with capped limit, got %d", respWithCap.StatusCode)
	}
	var cappedResult replayResponse
	cappedBody, err := io.ReadAll(respWithCap.Body)
	_ = respWithCap.Body.Close()
	if err != nil {
		t.Fatalf("read capped replay response body: %v", err)
	}
	if err := json.Unmarshal(cappedBody, &cappedResult); err != nil {
		t.Fatalf("decode capped replay response body: %v", err)
	}
	if cappedResult.RequestID != "replay-cap-1" ||
		cappedResult.RequestedLimit != 99999 ||
		cappedResult.EffectiveLimit != replayMaxLimit ||
		cappedResult.MaxLimit != replayMaxLimit ||
		!cappedResult.Capped {
		t.Fatalf("unexpected capped replay response: %+v", cappedResult)
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
	replayLog, ok := findLogEntry(logEntries, "admin replay dlq completed", func(entry map[string]any) bool {
		path, okPath := entry["path"].(string)
		method, okMethod := entry["method"].(string)
		requestID, okRequestID := entry["request_id"].(string)
		requested, okRequested := logFieldInt(entry, "requested_limit")
		effective, okEffective := logFieldInt(entry, "effective_limit")
		replayed, okReplayed := logFieldInt(entry, "replayed_count")
		ip, okIP := entry["requester_ip"].(string)
		userAgent, okUA := entry["user_agent"].(string)
		capped, okCapped := entry["capped"].(bool)
		durationMS, okDuration := logFieldInt(entry, "duration_ms")
		return okPath && okMethod && okRequestID && okRequested && okEffective && okReplayed && okIP && okUA && okCapped && okDuration &&
			path == "/admin/replay-dlq" &&
			method == http.MethodPost &&
			requestID == "replay-success-1" &&
			requested == 1 &&
			effective == 1 &&
			replayed == 1 &&
			ip == "203.0.113.52" &&
			userAgent == "tap-admin-test/success" &&
			!capped &&
			durationMS >= 0
	})
	if !ok {
		t.Fatalf("expected replay audit log with requester ip/effective limit/replayed count; logs=%s", logBuf.String())
	}
	if _, ok := findLogEntry(logEntries, "admin replay dlq completed", func(entry map[string]any) bool {
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
		t.Fatalf("expected capped replay completion audit log; logs=%s", logBuf.String())
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
	}, 2)
	assertMetricAtLeast(t, metricsText, "tap_admin_request_duration_seconds_count", map[string]string{
		"endpoint": adminEndpointReplayDLQ,
		"outcome":  adminOutcomeSuccess,
	}, 2)

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
			Stream:        "ENSEMBLE_TAP_CMD_TEST_POLLER_STATUS",
			SubjectPrefix: "ensemble.tap",
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
		_ = respNoToken.Body.Close()
		t.Fatalf("expected unauthorized status response to echo request id, got %q", got)
	}
	_ = respNoToken.Body.Close()
	if respNoToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
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

func intToString(v int) string {
	return fmt.Sprintf("%d", v)
}

func signGeneric(body []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
