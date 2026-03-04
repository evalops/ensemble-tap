//go:build integration

package publish

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/normalize"
	"github.com/nats-io/nats.go"
)

func TestRealNATSClickHousePipeline(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	clickhouseAddr := os.Getenv("CLICKHOUSE_ADDR")
	if natsURL == "" || clickhouseAddr == "" {
		t.Skip("NATS_URL and CLICKHOUSE_ADDR are required for integration test")
	}

	ctx := context.Background()
	streamName := "ENSEMBLE_TAP_REAL_IT"
	natsCfg := config.NATSConfig{
		URL:           natsURL,
		Stream:        streamName,
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   time.Minute,
	}
	publisher, err := NewNATSPublisher(ctx, natsCfg, nil)
	if err != nil {
		t.Fatalf("new nats publisher: %v", err)
	}
	defer publisher.Close()

	table := fmt.Sprintf("tap_events_it_%d", time.Now().UnixNano())
	clickCfg := config.ClickHouseConfig{
		Addr:          clickhouseAddr,
		Database:      "ensemble",
		Table:         table,
		BatchSize:     1,
		FlushInterval: 200 * time.Millisecond,
	}
	sink, err := NewClickHouseSink(ctx, clickCfg, natsCfg, publisher.JetStream(), nil)
	if err != nil {
		t.Fatalf("new clickhouse sink: %v", err)
	}
	defer sink.Close()
	if err := sink.Start(ctx); err != nil {
		t.Fatalf("start sink: %v", err)
	}

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "hubspot",
		EntityType:      "deal",
		EntityID:        "it_1",
		Action:          "updated",
		ProviderEventID: "evt_it_1",
		ProviderTime:    time.Now().UTC(),
		TenantID:        "tenant-it",
		Snapshot:        map[string]any{"stage": "open"},
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	if _, err := publisher.Publish(ctx, evt, "evt_it_1"); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{clickhouseAddr}, Auth: clickhouse.Auth{Database: "ensemble"}})
	if err != nil {
		t.Fatalf("open clickhouse conn: %v", err)
	}
	defer conn.Close()

	var count uint64
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		query := fmt.Sprintf("SELECT count() FROM ensemble.%s WHERE id='evt_it_1'", table)
		if err := conn.QueryRow(ctx, query).Scan(&count); err == nil && count >= 1 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("event was not persisted to clickhouse within timeout")
}

func init() {
	// Keep `nats` package in this integration build to ensure version compatibility.
	_ = nats.Msg{}
}
