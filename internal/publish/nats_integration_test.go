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
