package ingress

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	normproviders "github.com/evalops/ensemble-tap/internal/normalize/providers"
)

type fakePublisher struct {
	called    int
	lastEvent cloudevents.Event
	lastDedup string
	subject   string
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, event cloudevents.Event, dedupID string) (string, error) {
	f.called++
	f.lastEvent = event
	f.lastDedup = dedupID
	if f.err != nil {
		return "", f.err
	}
	if f.subject != "" {
		return f.subject, nil
	}
	return "ensemble.tap.test.event.updated", nil
}

func TestServerAcceptsGenericWebhook(t *testing.T) {
	secret := "super-secret"
	body := []byte(`{"id":"invoice_42","timestamp":"2026-03-03T14:22:00Z","amount":1200}`)

	pub := &fakePublisher{subject: "ensemble.tap.acme.invoice.paid"}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {Secret: secret, TenantID: "tenant-42"},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex(secret, body))
	req.Header.Set("X-Event-Type", "invoice.paid")
	req.Header.Set("X-Event-Id", "evt_123")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}
	if pub.called != 1 {
		t.Fatalf("expected publisher called once, got %d", pub.called)
	}
	if pub.lastDedup != "evt_123" {
		t.Fatalf("expected dedup to use provider event id, got %q", pub.lastDedup)
	}

	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "accepted" {
		t.Fatalf("unexpected status payload: %#v", resp)
	}
	if resp["subject"] != "ensemble.tap.acme.invoice.paid" {
		t.Fatalf("unexpected subject payload: %#v", resp)
	}
	if resp["id"] != "evt_123" {
		t.Fatalf("unexpected id payload: %#v", resp)
	}
}

func TestServerRejectsInvalidSignature(t *testing.T) {
	pub := &fakePublisher{}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {Secret: "real-secret"},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader([]byte(`{"id":"1"}`)))
	req.Header.Set("X-Signature", "sha256=deadbeef")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if pub.called != 0 {
		t.Fatalf("publisher should not be called when verification fails")
	}
}

func TestServerReturns500WhenPublishFails(t *testing.T) {
	secret := "super-secret"
	body := []byte(`{"id":"deal_1","timestamp":"2026-03-03T14:22:00Z"}`)

	pub := &fakePublisher{err: errors.New("nats unavailable")}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {Secret: secret},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex(secret, body))
	req.Header.Set("X-Event-Type", "deal.updated")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
	if pub.called != 1 {
		t.Fatalf("expected publisher called once, got %d", pub.called)
	}
}

func TestServerHashesDedupWhenEventIDMissing(t *testing.T) {
	secret := "super-secret"
	body := []byte(`{"id":"42","timestamp":"2026-03-03T14:22:00Z"}`)

	pub := &fakePublisher{}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {Secret: secret, TenantID: "tenant-1"},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex(secret, body))
	req.Header.Set("X-Event-Type", "deal.updated")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}

	normalized, err := normproviders.NormalizeGeneric("acme", "deal.updated", "", "", "tenant-1", body)
	if err != nil {
		t.Fatalf("normalize expected payload: %v", err)
	}
	expected := hashDedupID(normalized)
	if pub.lastDedup != expected {
		t.Fatalf("expected dedup hash %q, got %q", expected, pub.lastDedup)
	}
}

func TestServerReturns404WhenProviderNotConfigured(t *testing.T) {
	pub := &fakePublisher{}
	srv := newTestServer(map[string]config.ProviderConfig{}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/missing", bytes.NewReader([]byte(`{}`)))
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	if pub.called != 0 {
		t.Fatalf("publisher should not be called")
	}
}

func newTestServer(providers map[string]config.ProviderConfig, pub Publisher) *Server {
	cfg := config.Config{
		Providers: providers,
		Server: config.ServerConfig{
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
		},
	}
	cfg.ApplyDefaults()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(cfg, pub, nil, logger)
}

func signSHA256Hex(secret string, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
