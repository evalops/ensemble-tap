package publish

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/normalize"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestNATSPublisherPublishesAndDeduplicates(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_TEST",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_123",
		ProviderTime:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	subject, err := pub.Publish(ctx, evt, "dup_123")
	if err != nil {
		t.Fatalf("publish first message: %v", err)
	}
	if subject != "ensemble.tap.stripe.invoice.paid" {
		t.Fatalf("unexpected subject: %q", subject)
	}

	if _, err := pub.Publish(ctx, evt, "dup_123"); err != nil {
		t.Fatalf("publish duplicate message: %v", err)
	}

	info, err := pub.js.StreamInfo(cfg.Stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("expected 1 stored message after dedup, got %d", info.State.Msgs)
	}

	if err := pub.Ready(); err != nil {
		t.Fatalf("publisher should be ready: %v", err)
	}
}

func TestNATSPublisherEnsureStreamIsIdempotent(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_IDEMPOTENT",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   time.Minute,
	}

	ctx := context.Background()
	first, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("first publisher: %v", err)
	}
	defer first.Close()

	second, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("second publisher against existing stream: %v", err)
	}
	defer second.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:     "github",
		EntityType:   "issues",
		EntityID:     "7",
		Action:       "opened",
		ProviderTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	if _, err := second.Publish(ctx, evt, "idempotent_1"); err != nil {
		t.Fatalf("publish from second publisher: %v", err)
	}
}

func TestNATSPublisherTenantScopedSubject(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:                  s.ClientURL(),
		Stream:               "ENSEMBLE_TAP_TENANT_SUBJECT",
		SubjectPrefix:        "ensemble.tap",
		TenantScopedSubjects: true,
		MaxAge:               time.Hour,
		DedupWindow:          2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:     "stripe",
		EntityType:   "invoice",
		EntityID:     "in_1",
		Action:       "paid",
		ProviderTime: time.Now().UTC(),
		TenantID:     "tenant-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	subject, err := pub.Publish(ctx, evt, "tenant_subject_1")
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if subject != "ensemble.tap.tenant_1.stripe.invoice.paid" {
		t.Fatalf("unexpected subject: %q", subject)
	}
}

func TestNATSPublisherSetsRequestIDHeader(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_REQ_ID_HEADER",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_123",
		ProviderTime:    time.Now().UTC(),
		RequestID:       "req-nats-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	if _, err := pub.Publish(ctx, evt, "req_id_header_1"); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	stored, err := pub.js.GetMsg(cfg.Stream, 1)
	if err != nil {
		t.Fatalf("get stored message: %v", err)
	}
	if got := strings.TrimSpace(stored.Header.Get(natsRequestIDHeader)); got != "req-nats-1" {
		t.Fatalf("expected request id header req-nats-1, got %q", got)
	}
}

func TestNATSPublisherRawInfersRequestIDHeaderFromPayload(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_RAW_REQ_ID_HEADER",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "github",
		EntityType:      "issues",
		EntityID:        "7",
		Action:          "opened",
		ProviderEventID: "evt_gh_7",
		ProviderTime:    time.Now().UTC(),
		RequestID:       "req-raw-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal cloud event: %v", err)
	}

	if err := pub.PublishRaw(ctx, "ensemble.tap.github.issues.opened", payload, "raw_req_id_1", ""); err != nil {
		t.Fatalf("publish raw message: %v", err)
	}

	stored, err := pub.js.GetMsg(cfg.Stream, 1)
	if err != nil {
		t.Fatalf("get stored message: %v", err)
	}
	if got := strings.TrimSpace(stored.Header.Get(natsRequestIDHeader)); got != "req-raw-1" {
		t.Fatalf("expected request id header req-raw-1, got %q", got)
	}
}

func runNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("create nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server not ready")
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	return s
}

func TestNormalizeNATSRuntimeConfigDefaults(t *testing.T) {
	cfg := normalizeNATSRuntimeConfig(config.NATSConfig{})

	if cfg.ConnectTimeout != 5*time.Second {
		t.Fatalf("expected default connect timeout, got %s", cfg.ConnectTimeout)
	}
	if cfg.ReconnectWait != 2*time.Second {
		t.Fatalf("expected default reconnect wait, got %s", cfg.ReconnectWait)
	}
	if cfg.MaxReconnects != -1 {
		t.Fatalf("expected default max reconnects -1, got %d", cfg.MaxReconnects)
	}
	if cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("expected default publish timeout, got %s", cfg.PublishTimeout)
	}
	if cfg.StreamReplicas != 1 {
		t.Fatalf("expected default stream replicas 1, got %d", cfg.StreamReplicas)
	}
	if cfg.StreamStorage != "file" {
		t.Fatalf("expected default stream storage file, got %q", cfg.StreamStorage)
	}
	if cfg.StreamDiscard != "old" {
		t.Fatalf("expected default stream discard old, got %q", cfg.StreamDiscard)
	}
}

func TestStreamStorageAndDiscardPolicyMapping(t *testing.T) {
	if got := streamStorageType("memory"); got != nats.MemoryStorage {
		t.Fatalf("expected memory storage mapping, got %v", got)
	}
	if got := streamStorageType("file"); got != nats.FileStorage {
		t.Fatalf("expected file storage mapping, got %v", got)
	}
	if got := streamDiscardPolicy("new"); got != nats.DiscardNew {
		t.Fatalf("expected discard new mapping, got %v", got)
	}
	if got := streamDiscardPolicy("old"); got != nats.DiscardOld {
		t.Fatalf("expected discard old mapping, got %v", got)
	}
}
