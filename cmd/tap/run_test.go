package main

import (
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

func TestRunAdminReplayEndpointRequiresToken(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "ENSEMBLE_TAP_CMD_TEST_REPLAY",
			SubjectPrefix: "ensemble.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:        port,
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
			AdminToken:  "test-admin-token",
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

	replayURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq?limit=1"
	reqNoToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	respNoToken, err := http.DefaultClient.Do(reqNoToken)
	if err != nil {
		t.Fatalf("request replay without token: %v", err)
	}
	_ = respNoToken.Body.Close()
	if respNoToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
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
	respWithToken, err := http.DefaultClient.Do(reqWithToken)
	if err != nil {
		t.Fatalf("request replay with token: %v", err)
	}
	_ = respWithToken.Body.Close()
	if respWithToken.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with admin token, got %d", respWithToken.StatusCode)
	}

	got, err := replayedSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected replayed message: %v", err)
	}
	if string(got.Data) != string(payload) {
		t.Fatalf("unexpected replay payload: %s", string(got.Data))
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
