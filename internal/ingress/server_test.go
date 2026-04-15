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
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
	"github.com/evalops/siphon/internal/normalize"
	normproviders "github.com/evalops/siphon/internal/normalize/providers"
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
	return "siphon.tap.test.event.updated", nil
}

type fakeDLQRecorder struct {
	records []dlq.Record
}

func (f *fakeDLQRecorder) Record(_ context.Context, rec dlq.Record) error {
	f.records = append(f.records, rec)
	return nil
}

func TestServerAcceptsGenericWebhook(t *testing.T) {
	secret := "super-secret"
	body := []byte(`{"id":"invoice_42","timestamp":"2026-03-03T14:22:00Z","amount":1200}`)

	pub := &fakePublisher{subject: "siphon.tap.acme.invoice.paid"}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {Secret: secret, TenantID: "tenant-42"},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex(secret, body))
	req.Header.Set("X-Event-Type", "invoice.paid")
	req.Header.Set("X-Event-Id", "evt_123")
	req.Header.Set("X-Request-ID", "req-ingress-1")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}
	if pub.called != 1 {
		t.Fatalf("expected publisher called once, got %d", pub.called)
	}
	if pub.lastDedup != "tenant-42:evt_123" {
		t.Fatalf("expected dedup to use provider event id, got %q", pub.lastDedup)
	}

	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "accepted" {
		t.Fatalf("unexpected status payload: %#v", resp)
	}
	if resp["subject"] != "siphon.tap.acme.invoice.paid" {
		t.Fatalf("unexpected subject payload: %#v", resp)
	}
	if resp["id"] != "evt_123" {
		t.Fatalf("unexpected id payload: %#v", resp)
	}
	if resp["request_id"] != "req-ingress-1" {
		t.Fatalf("unexpected request id payload: %#v", resp)
	}
	if got := strings.TrimSpace(rr.Header().Get("X-Request-ID")); got != "req-ingress-1" {
		t.Fatalf("unexpected response request id header: %q", got)
	}

	data, err := normalize.DecodeTapEventData(pub.lastEvent)
	if err != nil {
		t.Fatalf("decode cloud event data: %v", err)
	}
	if data.RequestID != "req-ingress-1" {
		t.Fatalf("expected request id in cloud event data, got %q", data.RequestID)
	}
	extRaw, ok := pub.lastEvent.Extensions()[normalize.TapRequestIDExtension]
	if !ok {
		t.Fatalf("expected request id extension to be present")
	}
	ext, ok := extRaw.(string)
	if !ok {
		t.Fatalf("expected request id extension string, got %T", extRaw)
	}
	if got := strings.TrimSpace(ext); got != "req-ingress-1" {
		t.Fatalf("expected request id extension %q, got %q", "req-ingress-1", got)
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

func TestServerRecordsTenantScopedDLQSubjectOnPublishFailure(t *testing.T) {
	secret := "super-secret"
	body := []byte(`{"id":"deal_1","timestamp":"2026-03-03T14:22:00Z"}`)

	pub := &fakePublisher{err: errors.New("nats unavailable")}
	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"acme": {Secret: secret, TenantID: "tenant-a"},
		},
		NATS: config.NATSConfig{
			SubjectPrefix:        "custom.tap",
			TenantScopedSubjects: true,
		},
		Server: config.ServerConfig{
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
		},
	}
	cfg.ApplyDefaults()
	srv := NewServer(cfg, pub, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	dlqRecorder := &fakeDLQRecorder{}
	srv.SetDLQRecorder(dlqRecorder)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex(secret, body))
	req.Header.Set("X-Event-Type", "deal.updated")
	req.Header.Set("X-Event-Id", "evt_1")
	req.Header.Set("X-Correlation-ID", "corr-ingress-1")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
	if len(dlqRecorder.records) != 1 {
		t.Fatalf("expected one dlq record, got %d", len(dlqRecorder.records))
	}
	if got := dlqRecorder.records[0].OriginalSubject; got != "custom.tap.tenant_a.acme.deal.updated" {
		t.Fatalf("unexpected dlq replay subject: %q", got)
	}
	if got := dlqRecorder.records[0].RequestID; got != "corr-ingress-1" {
		t.Fatalf("unexpected dlq request id: %q", got)
	}
	if got := strings.TrimSpace(rr.Header().Get("X-Request-ID")); got != "corr-ingress-1" {
		t.Fatalf("unexpected response request id header: %q", got)
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
	if pub.lastDedup != "tenant-1:"+expected {
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

func TestServerSupportsTenantScopedWebhookCredentials(t *testing.T) {
	body := []byte(`{"id":"tenant_evt_1","timestamp":"2026-03-03T14:22:00Z"}`)
	pub := &fakePublisher{}
	srv := newTestServer(map[string]config.ProviderConfig{
		"acme": {
			Secret: "default-secret",
			Tenants: map[string]config.ProviderTenantConfig{
				"tenant-a": {Secret: "tenant-secret"},
			},
		},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme/tenant-a", bytes.NewReader(body))
	req.Header.Set("X-Signature", signSHA256Hex("tenant-secret", body))
	req.Header.Set("X-Event-Type", "deal.updated")
	req.Header.Set("X-Event-Id", "evt_tenant_a")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}
	if pub.lastDedup != "tenant-a:evt_tenant_a" {
		t.Fatalf("expected tenant scoped dedup id, got %q", pub.lastDedup)
	}
}

func TestServerAcceptsGitHubWebhookAfterBodyRead(t *testing.T) {
	secret := "github-secret"
	body := []byte(`{"action":"opened","issue":{"id":7}}`)

	pub := &fakePublisher{}
	srv := newTestServer(map[string]config.ProviderConfig{
		"github": {Secret: secret, TenantID: "tenant-gh"},
	}, pub)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signSHA256Hex(secret, body))
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}
	if pub.called != 1 {
		t.Fatalf("expected publisher called once, got %d", pub.called)
	}
	if pub.lastDedup != "tenant-gh:delivery-1" {
		t.Fatalf("expected github delivery id as dedup id, got %q", pub.lastDedup)
	}

	data, err := normalize.DecodeTapEventData(pub.lastEvent)
	if err != nil {
		t.Fatalf("decode cloud event data: %v", err)
	}
	if data.Provider != "github" || data.EntityType != "issues" || data.Action != "opened" {
		t.Fatalf("unexpected github event data: %+v", data)
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
