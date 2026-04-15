package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/evalops/siphon/config"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestDLQRecordAndReplay(t *testing.T) {
	s := runNATSServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cfg := config.NATSConfig{Stream: "SIPHON", SubjectPrefix: "siphon.tap"}
	p, err := NewPublisher(context.Background(), cfg, js)
	if err != nil {
		t.Fatalf("new dlq publisher: %v", err)
	}

	rec := Record{
		Stage:           "publish",
		Provider:        "hubspot",
		RequestID:       "req-dlq-1",
		Reason:          "nats timeout",
		OriginalSubject: "siphon.tap.hubspot.deal.updated",
		OriginalPayload: []byte(`{"id":"evt_1"}`),
		OriginalDedupID: "evt_1",
	}
	if err := p.Record(context.Background(), rec); err != nil {
		t.Fatalf("record dlq: %v", err)
	}

	replayed := 0
	count, err := p.Replay(context.Background(), 10, func(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error {
		replayed++
		if subject != rec.OriginalSubject || dedupID != rec.OriginalDedupID {
			t.Fatalf("unexpected replay record: %s %s", subject, dedupID)
		}
		if requestID != rec.RequestID {
			t.Fatalf("unexpected replay request id: %q", requestID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay dlq: %v", err)
	}
	if count != 1 || replayed != 1 {
		t.Fatalf("expected one replayed message, got count=%d replayed=%d", count, replayed)
	}
}

func TestDLQPending(t *testing.T) {
	s := runNATSServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cfg := config.NATSConfig{Stream: "SIPHON_PENDING", SubjectPrefix: "siphon.tap"}
	p, err := NewPublisher(context.Background(), cfg, js)
	if err != nil {
		t.Fatalf("new dlq publisher: %v", err)
	}

	rec := Record{
		Stage:           "publish",
		Provider:        "stripe",
		Reason:          "forced test",
		OriginalSubject: "siphon.tap.stripe.invoice.updated",
		OriginalPayload: []byte(`{"id":"evt_pending_1"}`),
		OriginalDedupID: "evt_pending_1",
	}
	if err := p.Record(context.Background(), rec); err != nil {
		t.Fatalf("record dlq: %v", err)
	}
	pending, err := p.Pending()
	if err != nil {
		t.Fatalf("read pending: %v", err)
	}
	if pending < 1 {
		t.Fatalf("expected pending >= 1, got %d", pending)
	}

	if _, err := p.Replay(context.Background(), 10, func(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error {
		return nil
	}); err != nil {
		t.Fatalf("replay dlq: %v", err)
	}

	pendingAfter, err := p.Pending()
	if err != nil {
		t.Fatalf("read pending after replay: %v", err)
	}
	if pendingAfter != 0 {
		t.Fatalf("expected no pending records after replay, got %d", pendingAfter)
	}
}

func runNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()}
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
