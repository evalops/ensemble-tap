package publish

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/ingress"
)

func TestWebhookToNATSToClickHousePipeline(t *testing.T) {
	s := runNATSServer(t)
	streamName := "SIPHON_BLACKBOX"
	natsCfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        streamName,
		SubjectPrefix: "siphon.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewNATSPublisher(ctx, natsCfg, nil)
	if err != nil {
		t.Fatalf("new nats publisher: %v", err)
	}
	defer publisher.Close()

	var (
		mu       sync.Mutex
		inserted []clickhouseRow
	)
	insertedCh := make(chan struct{}, 1)
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{
			BatchSize:     1,
			FlushInterval: 20 * time.Millisecond,
		},
		natsCfg: natsCfg,
		js:      publisher.JetStream(),
		insertRowsFn: func(_ context.Context, rows []clickhouseRow) error {
			mu.Lock()
			defer mu.Unlock()
			inserted = append(inserted, append([]clickhouseRow(nil), rows...)...)
			select {
			case insertedCh <- struct{}{}:
			default:
			}
			return nil
		},
	}
	if err := sink.Start(ctx); err != nil {
		t.Fatalf("start clickhouse sink: %v", err)
	}
	defer sink.Close()

	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"acme": {Secret: "test-secret", TenantID: "tenant-1"},
		},
		NATS: natsCfg,
		Server: config.ServerConfig{
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
		},
	}
	cfg.ApplyDefaults()

	srv := ingress.NewServer(cfg, publisher, nil, nil)
	body := []byte(`{"id":"42","timestamp":"2026-03-03T14:22:00Z","amount":85000}`)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signGeneric(body, "test-secret"))
	req.Header.Set("X-Event-Type", "deal.updated")
	req.Header.Set("X-Event-Id", "evt_blackbox_1")

	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected webhook accepted, got %d (%s)", rr.Code, rr.Body.String())
	}

	select {
	case <-insertedCh:
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for clickhouse sink insert")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(inserted) == 0 {
		t.Fatalf("expected at least one inserted row")
	}
	row := inserted[0]
	if row.Provider != "acme" || row.EntityType != "deal" || row.Action != "updated" {
		t.Fatalf("unexpected row mapping: %+v", row)
	}
	if row.EntityID != "42" {
		t.Fatalf("unexpected entity id: %q", row.EntityID)
	}
	if row.ProviderEventID != "evt_blackbox_1" {
		t.Fatalf("unexpected provider event id: %q", row.ProviderEventID)
	}
	if row.TenantID == nil || *row.TenantID != "tenant-1" {
		t.Fatalf("unexpected tenant id: %+v", row.TenantID)
	}
}

func signGeneric(body []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
