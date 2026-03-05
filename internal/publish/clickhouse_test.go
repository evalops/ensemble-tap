package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/normalize"
)

func TestEventToRowMapsCloudEventFields(t *testing.T) {
	when := time.Date(2026, 3, 3, 14, 22, 0, 0, time.UTC)
	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_456",
		ProviderTime:    when,
		TenantID:        "tenant-1",
		Changes: map[string]normalize.FieldChange{
			"status": {From: "open", To: "paid"},
		},
		Snapshot: map[string]any{"amount": 1200},
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	payload, _ := json.Marshal(evt)

	row, err := eventToRow(payload)
	if err != nil {
		t.Fatalf("eventToRow failed: %v", err)
	}

	if row.ID != "evt_456" {
		t.Fatalf("unexpected row ID: %q", row.ID)
	}
	if row.Provider != "stripe" || row.EntityType != "invoice" || row.Action != "paid" {
		t.Fatalf("unexpected row mapping: %+v", row)
	}
	if row.TenantID == nil || *row.TenantID != "tenant-1" {
		t.Fatalf("expected tenant_id to be mapped")
	}
	if row.Time.UTC() != when {
		t.Fatalf("expected row time %s, got %s", when, row.Time)
	}
	if row.Changes == "" || row.Snapshot == "" {
		t.Fatalf("expected JSON fields to be populated")
	}
}

func TestEventToRowUsesCurrentTimeWhenCloudEventTimeMissing(t *testing.T) {
	evt := cloudevents.NewEvent()
	evt.SetSpecVersion(cloudevents.VersionV1)
	evt.SetID("evt_no_time")
	evt.SetType("ensemble.tap.acme.deal.updated")
	evt.SetSource("tap/acme/default")
	evt.SetSubject("deal/1")
	if err := evt.SetData(cloudevents.ApplicationJSON, normalize.TapEventData{
		Provider:   "acme",
		EntityType: "deal",
		EntityID:   "1",
		Action:     "updated",
	}); err != nil {
		t.Fatalf("set event data: %v", err)
	}

	payload, _ := json.Marshal(evt)
	before := time.Now().UTC().Add(-2 * time.Second)
	row, err := eventToRow(payload)
	if err != nil {
		t.Fatalf("eventToRow failed: %v", err)
	}
	if row.Time.Before(before) || row.Time.IsZero() {
		t.Fatalf("expected fallback row time, got %s", row.Time)
	}
	if row.TenantID != nil {
		t.Fatalf("expected nil tenant_id when missing")
	}
}

func TestEventToRowRejectsInvalidPayload(t *testing.T) {
	if _, err := eventToRow([]byte(`not-json`)); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestClickHouseSinkRetriesAfterInsertFailure(t *testing.T) {
	s := runNATSServer(t)
	natsCfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_CLICKHOUSE_RETRY",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   time.Minute,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewNATSPublisher(ctx, natsCfg, nil)
	if err != nil {
		t.Fatalf("new nats publisher: %v", err)
	}
	defer publisher.Close()

	inserted := make(chan struct{}, 1)
	var insertCalls atomic.Int32
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{
			BatchSize:     1,
			FlushInterval: 20 * time.Millisecond,
		},
		natsCfg: natsCfg,
		js:      publisher.JetStream(),
		insertRowsFn: func(_ context.Context, rows []clickhouseRow) error {
			call := insertCalls.Add(1)
			if call == 1 {
				return fmt.Errorf("forced insert failure")
			}
			select {
			case inserted <- struct{}{}:
			default:
			}
			return nil
		},
	}
	if err := sink.Start(ctx); err != nil {
		t.Fatalf("start clickhouse sink: %v", err)
	}
	defer sink.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "hubspot",
		EntityType:      "deal",
		EntityID:        "retry-1",
		Action:          "updated",
		ProviderEventID: "evt_retry_1",
		ProviderTime:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}
	if _, err := publisher.Publish(ctx, evt, "evt_retry_1"); err != nil {
		t.Fatalf("publish cloud event: %v", err)
	}

	select {
	case <-inserted:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for retry insert success")
	}
	if got := insertCalls.Load(); got < 2 {
		t.Fatalf("expected at least 2 insert attempts, got %d", got)
	}
}

func TestNormalizeClickHouseRuntimeConfigDefaults(t *testing.T) {
	cfg := normalizeClickHouseRuntimeConfig(config.ClickHouseConfig{})

	if cfg.Username != "default" {
		t.Fatalf("expected default username, got %q", cfg.Username)
	}
	if cfg.DialTimeout != 5*time.Second {
		t.Fatalf("expected default dial timeout, got %s", cfg.DialTimeout)
	}
	if cfg.MaxOpenConns != 4 {
		t.Fatalf("expected default max open conns, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 2 {
		t.Fatalf("expected default max idle conns, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("expected default conn max lifetime, got %s", cfg.ConnMaxLifetime)
	}
	if cfg.BatchSize != 500 {
		t.Fatalf("expected default batch size, got %d", cfg.BatchSize)
	}
	if cfg.FlushInterval != 2*time.Second {
		t.Fatalf("expected default flush interval, got %s", cfg.FlushInterval)
	}
	if cfg.ConsumerFetchBatch != 100 {
		t.Fatalf("expected default consumer fetch batch, got %d", cfg.ConsumerFetchBatch)
	}
	if cfg.ConsumerFetchMaxWait != 500*time.Millisecond {
		t.Fatalf("expected default consumer fetch max wait, got %s", cfg.ConsumerFetchMaxWait)
	}
	if cfg.ConsumerAckWait != 30*time.Second {
		t.Fatalf("expected default consumer ack wait, got %s", cfg.ConsumerAckWait)
	}
	if cfg.ConsumerMaxAckPending != 1000 {
		t.Fatalf("expected default consumer max ack pending, got %d", cfg.ConsumerMaxAckPending)
	}
	if cfg.InsertTimeout != 10*time.Second {
		t.Fatalf("expected default insert timeout, got %s", cfg.InsertTimeout)
	}
	if cfg.ConsumerName != "tap_clickhouse_sink" {
		t.Fatalf("expected default consumer name, got %q", cfg.ConsumerName)
	}
	if cfg.RetentionTTL != 365*24*time.Hour {
		t.Fatalf("expected default retention ttl 365d, got %s", cfg.RetentionTTL)
	}
}

func TestNormalizeClickHouseRuntimeConfigTrimsTLSFields(t *testing.T) {
	cfg := normalizeClickHouseRuntimeConfig(config.ClickHouseConfig{
		TLSServerName: " clickhouse.internal ",
		CAFile:        " /tmp/ca.pem ",
		CertFile:      " /tmp/client.crt ",
		KeyFile:       " /tmp/client.key ",
	})
	if cfg.TLSServerName != "clickhouse.internal" {
		t.Fatalf("expected trimmed tls server name, got %q", cfg.TLSServerName)
	}
	if cfg.CAFile != "/tmp/ca.pem" {
		t.Fatalf("expected trimmed ca file, got %q", cfg.CAFile)
	}
	if cfg.CertFile != "/tmp/client.crt" {
		t.Fatalf("expected trimmed cert file, got %q", cfg.CertFile)
	}
	if cfg.KeyFile != "/tmp/client.key" {
		t.Fatalf("expected trimmed key file, got %q", cfg.KeyFile)
	}
}

func TestClickHouseTLSConfigSelection(t *testing.T) {
	tlsCfg, err := clickHouseTLSConfig(config.ClickHouseConfig{})
	if err != nil {
		t.Fatalf("expected no tls config error, got %v", err)
	}
	if tlsCfg != nil {
		t.Fatalf("expected nil tls config for insecure defaults")
	}

	tlsCfg, err = clickHouseTLSConfig(config.ClickHouseConfig{Secure: true, TLSServerName: "clickhouse.internal"})
	if err != nil {
		t.Fatalf("expected secure tls config without error, got %v", err)
	}
	if tlsCfg == nil {
		t.Fatalf("expected tls config when secure=true")
	}
	if tlsCfg.ServerName != "clickhouse.internal" {
		t.Fatalf("expected tls server name clickhouse.internal, got %q", tlsCfg.ServerName)
	}

	_, err = clickHouseTLSConfig(config.ClickHouseConfig{Secure: true, CAFile: "/tmp/not-found-ca.pem"})
	if err == nil {
		t.Fatalf("expected missing ca_file to error")
	}

	badCA := t.TempDir() + "/bad-ca.pem"
	if writeErr := os.WriteFile(badCA, []byte("not-a-cert"), 0o600); writeErr != nil {
		t.Fatalf("write bad ca file: %v", writeErr)
	}
	_, err = clickHouseTLSConfig(config.ClickHouseConfig{Secure: true, CAFile: badCA})
	if err == nil {
		t.Fatalf("expected malformed ca_file to error")
	}
}

func TestClickHouseAddresses(t *testing.T) {
	addrs := clickHouseAddresses(" clickhouse-a:9000, clickhouse-b:9000 ")
	if len(addrs) != 2 {
		t.Fatalf("expected 2 clickhouse addresses, got %d", len(addrs))
	}
	if addrs[0] != "clickhouse-a:9000" || addrs[1] != "clickhouse-b:9000" {
		t.Fatalf("unexpected clickhouse addresses: %#v", addrs)
	}
}
