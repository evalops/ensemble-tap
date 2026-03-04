package publish

import (
	"context"
	"testing"
	"time"

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/normalize"
	natsserver "github.com/nats-io/nats-server/v2/server"
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
