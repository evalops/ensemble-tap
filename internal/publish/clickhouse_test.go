package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chcolumn "github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/normalize"
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

func TestEventToRowSupportsLegacyJSONCloudEventData(t *testing.T) {
	evt := cloudevents.NewEvent()
	evt.SetSpecVersion(cloudevents.VersionV1)
	evt.SetID("evt_legacy_json")
	evt.SetType("siphon.tap.acme.deal.updated")
	evt.SetSource("tap/acme/default")
	evt.SetSubject("deal/1")
	evt.SetTime(time.Date(2026, 3, 3, 14, 22, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, normalize.TapEventData{
		Provider:        "acme",
		EntityType:      "deal",
		EntityID:        "1",
		Action:          "updated",
		ProviderEventID: "legacy-1",
		TenantID:        "tenant-legacy",
	}); err != nil {
		t.Fatalf("set legacy event data: %v", err)
	}

	payload, _ := json.Marshal(evt)
	row, err := eventToRow(payload)
	if err != nil {
		t.Fatalf("eventToRow failed for legacy json event: %v", err)
	}
	if row.Provider != "acme" || row.ProviderEventID != "legacy-1" {
		t.Fatalf("unexpected row mapping from legacy event: %+v", row)
	}
}

func TestEventToRowUsesCurrentTimeWhenCloudEventTimeMissing(t *testing.T) {
	evt := cloudevents.NewEvent()
	evt.SetSpecVersion(cloudevents.VersionV1)
	evt.SetID("evt_no_time")
	evt.SetType("siphon.tap.acme.deal.updated")
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
		Stream:        "SIPHON_CLICKHOUSE_RETRY",
		SubjectPrefix: "siphon.tap",
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

func TestNewClickHouseSinkRejectsInvalidTLSFiles(t *testing.T) {
	sink, err := NewClickHouseSink(
		context.Background(),
		config.ClickHouseConfig{
			Addr:     "127.0.0.1:9000",
			Database: "siphon",
			Table:    "tap_events",
			Secure:   true,
			CAFile:   "/tmp/does-not-exist-ca.pem",
		},
		config.NATSConfig{},
		nil,
		nil,
	)
	if err == nil {
		if sink != nil {
			sink.Close()
		}
		t.Fatalf("expected clickhouse tls configuration error")
	}
	if !strings.Contains(err.Error(), "configure clickhouse tls") {
		t.Fatalf("expected configure clickhouse tls error, got %v", err)
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

func TestNewClickHouseSinkReturnsNilWhenAddressUnset(t *testing.T) {
	sink, err := NewClickHouseSink(context.Background(), config.ClickHouseConfig{}, config.NATSConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("expected no error when clickhouse is disabled, got %v", err)
	}
	if sink != nil {
		t.Fatalf("expected nil sink when clickhouse.addr is not configured")
	}
}

func TestClickHouseSinkReady(t *testing.T) {
	t.Run("running insert mock without connection", func(t *testing.T) {
		sink := &ClickHouseSink{
			insertRowsFn: func(context.Context, []clickhouseRow) error { return nil },
		}
		sink.running.Store(true)
		if err := sink.Ready(); err != nil {
			t.Fatalf("expected sink to be ready with insertRowsFn fallback, got %v", err)
		}
	})

	t.Run("fails when not running", func(t *testing.T) {
		sink := &ClickHouseSink{
			conn: &mockClickHouseConn{
				queryRowFn: func(context.Context, string, ...any) chdriver.Row {
					return &mockReadyRow{value: 1}
				},
			},
		}
		if err := sink.Ready(); err == nil {
			t.Fatalf("expected sink readiness to fail when not running")
		}
	})

	t.Run("fails when health query errors", func(t *testing.T) {
		sink := &ClickHouseSink{
			conn: &mockClickHouseConn{
				queryRowFn: func(context.Context, string, ...any) chdriver.Row {
					return &mockReadyRow{err: fmt.Errorf("ping failed")}
				},
			},
		}
		sink.running.Store(true)
		if err := sink.Ready(); err == nil || !strings.Contains(err.Error(), "clickhouse health check failed") {
			t.Fatalf("expected clickhouse health check error, got %v", err)
		}
	})

	t.Run("fails on unexpected response value", func(t *testing.T) {
		sink := &ClickHouseSink{
			conn: &mockClickHouseConn{
				queryRowFn: func(context.Context, string, ...any) chdriver.Row {
					return &mockReadyRow{value: 2}
				},
			},
		}
		sink.running.Store(true)
		if err := sink.Ready(); err == nil || !strings.Contains(err.Error(), "unexpected result") {
			t.Fatalf("expected unexpected-result readiness error, got %v", err)
		}
	})

	t.Run("healthy query returns ready", func(t *testing.T) {
		sink := &ClickHouseSink{
			conn: &mockClickHouseConn{
				queryRowFn: func(context.Context, string, ...any) chdriver.Row {
					return &mockReadyRow{value: 1}
				},
			},
		}
		sink.running.Store(true)
		if err := sink.Ready(); err != nil {
			t.Fatalf("expected sink to be ready, got %v", err)
		}
	})
}

func TestInitSchemaExecutesDDL(t *testing.T) {
	executed := make([]string, 0, 2)
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{
			Database:     "siphon",
			Table:        "tap_events",
			RetentionTTL: 24 * time.Hour,
		},
		conn: &mockClickHouseConn{
			execFn: func(_ context.Context, query string, _ ...any) error {
				executed = append(executed, query)
				return nil
			},
		},
	}

	if err := sink.initSchema(context.Background()); err != nil {
		t.Fatalf("initSchema failed: %v", err)
	}
	if len(executed) != 2 {
		t.Fatalf("expected 2 schema statements, got %d", len(executed))
	}
	if !strings.Contains(executed[0], "CREATE DATABASE IF NOT EXISTS siphon") {
		t.Fatalf("expected create database statement, got %q", executed[0])
	}
	if !strings.Contains(executed[1], "CREATE TABLE IF NOT EXISTS siphon.tap_events") {
		t.Fatalf("expected create table statement, got %q", executed[1])
	}
}

func TestInitSchemaReturnsCreateDatabaseError(t *testing.T) {
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{Database: "siphon", Table: "tap_events"},
		conn: &mockClickHouseConn{
			execFn: func(_ context.Context, query string, _ ...any) error {
				return fmt.Errorf("db exec failed")
			},
		},
	}
	if err := sink.initSchema(context.Background()); err == nil {
		t.Fatalf("expected schema bootstrap error")
	}
}

func TestInsertRowsErrorsWhenNoConnectionConfigured(t *testing.T) {
	sink := &ClickHouseSink{}
	err := sink.insertRows(context.Background(), []clickhouseRow{{ID: "evt_1"}})
	if err == nil || !strings.Contains(err.Error(), "connection is not configured") {
		t.Fatalf("expected missing connection error, got %v", err)
	}
}

func TestInsertRowsUsesPreparedBatch(t *testing.T) {
	batch := &mockBatch{}
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{Database: "siphon", Table: "tap_events"},
		conn: &mockClickHouseConn{
			prepareBatchFn: func(_ context.Context, query string, _ ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
				if !strings.Contains(query, "INSERT INTO siphon.tap_events") {
					t.Fatalf("unexpected insert query: %q", query)
				}
				return batch, nil
			},
		},
	}

	err := sink.insertRows(context.Background(), []clickhouseRow{
		{
			ID:              "evt_1",
			Type:            "siphon.tap.acme.deal.updated",
			Source:          "tap/acme/default",
			Subject:         "deal/1",
			Time:            time.Now().UTC(),
			Provider:        "acme",
			EntityType:      "deal",
			EntityID:        "1",
			Action:          "updated",
			Changes:         "{}",
			Snapshot:        "{}",
			ProviderEventID: "evt_1",
		},
	})
	if err != nil {
		t.Fatalf("insertRows failed: %v", err)
	}
	if batch.appendCalls != 1 {
		t.Fatalf("expected 1 appended row, got %d", batch.appendCalls)
	}
	if !batch.sent {
		t.Fatalf("expected batch send to be called")
	}
}

func TestNormalizeClickHouseRuntimeConfigConsumerControls(t *testing.T) {
	cfg := normalizeClickHouseRuntimeConfig(config.ClickHouseConfig{
		MaxOpenConns:               2,
		MaxIdleConns:               5,
		ConnMaxLifetime:            -1 * time.Second,
		ConsumerBackoff:            []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
		ConsumerMaxDeliver:         0,
		ConsumerMaxWaiting:         -1,
		ConsumerMaxRequestMaxBytes: -5,
	})

	if cfg.MaxIdleConns != 2 {
		t.Fatalf("expected max_idle_conns clamped to max_open_conns, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("expected default conn_max_lifetime, got %s", cfg.ConnMaxLifetime)
	}
	if cfg.ConsumerMaxDeliver != 2 {
		t.Fatalf("expected consumer_max_deliver inferred from backoff length, got %d", cfg.ConsumerMaxDeliver)
	}
	if cfg.ConsumerMaxWaiting != 0 {
		t.Fatalf("expected consumer_max_waiting normalized to 0, got %d", cfg.ConsumerMaxWaiting)
	}
	if cfg.ConsumerMaxRequestMaxBytes != 0 {
		t.Fatalf("expected consumer_max_request_max_bytes normalized to 0, got %d", cfg.ConsumerMaxRequestMaxBytes)
	}
}

func TestFilterDuplicateRowsInBatch(t *testing.T) {
	sink := &ClickHouseSink{}
	rows := []clickhouseRow{
		{ID: "evt_1"},
		{ID: "evt_1"},
		{ID: ""},
		{ID: "evt_2"},
	}

	filtered, skipped, err := sink.filterDuplicateRows(context.Background(), rows)
	if err != nil {
		t.Fatalf("filterDuplicateRows failed: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("expected 1 skipped duplicate row, got %d", skipped)
	}
	if len(filtered) != 3 {
		t.Fatalf("expected 3 rows after in-batch dedupe, got %d", len(filtered))
	}
}

func TestFilterDuplicateRowsAgainstExistingIDs(t *testing.T) {
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{Database: "siphon", Table: "tap_events"},
		conn: &mockClickHouseConn{
			queryFn: func(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
				if !strings.Contains(query, "SELECT id FROM siphon.tap_events") {
					t.Fatalf("unexpected dedupe lookup query: %q", query)
				}
				return &mockRows{values: []string{"evt_2"}}, nil
			},
		},
	}
	rows := []clickhouseRow{
		{ID: "evt_1"},
		{ID: "evt_1"},
		{ID: "evt_2"},
		{ID: ""},
	}

	filtered, skipped, err := sink.filterDuplicateRows(context.Background(), rows)
	if err != nil {
		t.Fatalf("filterDuplicateRows failed: %v", err)
	}
	if skipped != 2 {
		t.Fatalf("expected 2 skipped rows (batch + persisted duplicate), got %d", skipped)
	}
	if len(filtered) != 2 {
		t.Fatalf("expected 2 rows after dedupe, got %d", len(filtered))
	}
}

func TestFilterDuplicateRowsReturnsLookupError(t *testing.T) {
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{Database: "siphon", Table: "tap_events"},
		conn: &mockClickHouseConn{
			queryFn: func(_ context.Context, _ string, _ ...any) (chdriver.Rows, error) {
				return nil, fmt.Errorf("lookup failed")
			},
		},
	}
	_, _, err := sink.filterDuplicateRows(context.Background(), []clickhouseRow{{ID: "evt_1"}})
	if err == nil {
		t.Fatalf("expected dedupe lookup error")
	}
}

func TestLoadExistingRowIDsChunkingAndErrors(t *testing.T) {
	calls := 0
	sink := &ClickHouseSink{
		cfg: config.ClickHouseConfig{Database: "siphon", Table: "tap_events"},
		conn: &mockClickHouseConn{
			queryFn: func(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
				calls++
				if !strings.Contains(query, "SELECT id FROM siphon.tap_events WHERE id IN (") {
					t.Fatalf("unexpected query: %q", query)
				}
				return &mockRows{values: []string{"evt_existing"}}, nil
			},
		},
	}
	ids := make([]string, 0, 205)
	for i := 0; i < 205; i++ {
		ids = append(ids, fmt.Sprintf("evt_%03d", i))
	}

	existing, err := sink.loadExistingRowIDs(context.Background(), ids)
	if err != nil {
		t.Fatalf("loadExistingRowIDs failed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 chunked query calls, got %d", calls)
	}
	if _, ok := existing["evt_existing"]; !ok {
		t.Fatalf("expected returned id set to include evt_existing")
	}

	sink.conn = &mockClickHouseConn{
		queryFn: func(_ context.Context, _ string, _ ...any) (chdriver.Rows, error) {
			return &mockRows{values: []string{"evt_1"}, scanErr: fmt.Errorf("scan failed")}, nil
		},
	}
	if _, err := sink.loadExistingRowIDs(context.Background(), []string{"evt_1"}); err == nil {
		t.Fatalf("expected scan error")
	}

	sink.conn = &mockClickHouseConn{
		queryFn: func(_ context.Context, _ string, _ ...any) (chdriver.Rows, error) {
			return &mockRows{values: []string{"evt_1"}, err: fmt.Errorf("iter failed")}, nil
		},
	}
	if _, err := sink.loadExistingRowIDs(context.Background(), []string{"evt_1"}); err == nil {
		t.Fatalf("expected iterator error")
	}

	sink.conn = &mockClickHouseConn{
		queryFn: func(_ context.Context, _ string, _ ...any) (chdriver.Rows, error) {
			return &mockRows{values: []string{"evt_1"}, closeErr: fmt.Errorf("close failed")}, nil
		},
	}
	if _, err := sink.loadExistingRowIDs(context.Background(), []string{"evt_1"}); err == nil {
		t.Fatalf("expected close error")
	}
}

func TestQuotedStringListAndChunkStrings(t *testing.T) {
	quoted := quotedStringList([]string{"evt_1", "  ", "evt_'2"})
	if quoted != "'evt_1','evt_''2'" {
		t.Fatalf("unexpected quoted list: %q", quoted)
	}
	if empty := quotedStringList(nil); empty != "''" {
		t.Fatalf("expected quoted empty placeholder, got %q", empty)
	}

	chunks := chunkStrings([]string{"a", "b", "c", "d", "e"}, 2)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if len(chunks[0]) != 2 || len(chunks[1]) != 2 || len(chunks[2]) != 1 {
		t.Fatalf("unexpected chunk sizes: %#v", chunks)
	}
	if got := chunkStrings([]string{"a"}, 0); got != nil {
		t.Fatalf("expected nil chunks for invalid chunk size")
	}
}

func TestFallbackEventIDFromPayloadDeterministic(t *testing.T) {
	payload := []byte(`{"event":"demo"}`)
	first := fallbackEventIDFromPayload(payload)
	second := fallbackEventIDFromPayload(payload)
	if first != second {
		t.Fatalf("expected deterministic hash-based fallback id")
	}
	if !strings.HasPrefix(first, "evt_") {
		t.Fatalf("expected fallback id to be prefixed with evt_, got %q", first)
	}
}

func TestEventToRowFallbackIDPriority(t *testing.T) {
	withProviderEventID := []byte(`{
  "specversion":"1.0",
  "type":"siphon.tap.acme.deal.updated",
  "source":"tap/acme/default",
  "subject":"deal/1",
  "data":{
    "provider":"acme",
    "entity_type":"deal",
    "entity_id":"1",
    "action":"updated",
    "provider_event_id":"evt_provider"
  }
}`)
	row, err := eventToRow(withProviderEventID)
	if err != nil {
		t.Fatalf("eventToRow with provider_event_id failed: %v", err)
	}
	if row.ID != "evt_provider" {
		t.Fatalf("expected provider_event_id fallback id, got %q", row.ID)
	}

	withoutIDs := []byte(`{
  "specversion":"1.0",
  "type":"siphon.tap.acme.deal.updated",
  "source":"tap/acme/default",
  "subject":"deal/1",
  "data":{
    "provider":"acme",
    "entity_type":"deal",
    "entity_id":"1",
    "action":"updated"
  }
}`)
	row, err = eventToRow(withoutIDs)
	if err != nil {
		t.Fatalf("eventToRow without ids failed: %v", err)
	}
	if want := fallbackEventIDFromPayload(withoutIDs); row.ID != want {
		t.Fatalf("expected payload-hash fallback id %q, got %q", want, row.ID)
	}
}

type mockClickHouseConn struct {
	queryFn        func(context.Context, string, ...any) (chdriver.Rows, error)
	queryRowFn     func(context.Context, string, ...any) chdriver.Row
	prepareBatchFn func(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error)
	execFn         func(context.Context, string, ...any) error
}

func (m *mockClickHouseConn) Contributors() []string { return nil }

func (m *mockClickHouseConn) ServerVersion() (*chdriver.ServerVersion, error) { return nil, nil }

func (m *mockClickHouseConn) Select(_ context.Context, _ any, _ string, _ ...any) error { return nil }

func (m *mockClickHouseConn) Query(ctx context.Context, query string, args ...any) (chdriver.Rows, error) {
	if m.queryFn == nil {
		return nil, fmt.Errorf("query not configured")
	}
	return m.queryFn(ctx, query, args...)
}

func (m *mockClickHouseConn) QueryRow(ctx context.Context, query string, args ...any) chdriver.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return &mockRow{err: fmt.Errorf("queryrow not configured")}
}

func (m *mockClickHouseConn) PrepareBatch(ctx context.Context, query string, opts ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	if m.prepareBatchFn == nil {
		return nil, fmt.Errorf("preparebatch not configured")
	}
	return m.prepareBatchFn(ctx, query, opts...)
}

func (m *mockClickHouseConn) Exec(ctx context.Context, query string, args ...any) error {
	if m.execFn == nil {
		return nil
	}
	return m.execFn(ctx, query, args...)
}

func (m *mockClickHouseConn) AsyncInsert(_ context.Context, _ string, _ bool, _ ...any) error {
	return nil
}

func (m *mockClickHouseConn) Ping(context.Context) error { return nil }

func (m *mockClickHouseConn) Stats() chdriver.Stats { return chdriver.Stats{} }

func (m *mockClickHouseConn) Close() error { return nil }

type mockRows struct {
	values   []string
	index    int
	scanErr  error
	err      error
	closeErr error
}

func (m *mockRows) Next() bool {
	if m.index >= len(m.values) {
		return false
	}
	m.index++
	return true
}

func (m *mockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) != 1 {
		return fmt.Errorf("expected single destination for id scan")
	}
	target, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("expected *string scan destination")
	}
	*target = m.values[m.index-1]
	return nil
}

func (m *mockRows) ScanStruct(any) error { return nil }

func (m *mockRows) ColumnTypes() []chdriver.ColumnType { return nil }

func (m *mockRows) Totals(...any) error { return nil }

func (m *mockRows) Columns() []string { return []string{"id"} }

func (m *mockRows) HasData() bool { return len(m.values) > 0 }

func (m *mockRows) Close() error { return m.closeErr }

func (m *mockRows) Err() error { return m.err }

type mockRow struct {
	err error
}

func (m *mockRow) Err() error { return m.err }

func (m *mockRow) Scan(...any) error { return m.err }

func (m *mockRow) ScanStruct(any) error { return m.err }

type mockBatch struct {
	appendCalls int
	sent        bool
}

func (m *mockBatch) Abort() error { return nil }

func (m *mockBatch) Append(...any) error {
	m.appendCalls++
	return nil
}

func (m *mockBatch) AppendStruct(any) error { return nil }

func (m *mockBatch) Column(int) chdriver.BatchColumn { return &mockBatchColumn{} }

func (m *mockBatch) Flush() error { return nil }

func (m *mockBatch) Send() error {
	m.sent = true
	return nil
}

func (m *mockBatch) IsSent() bool { return m.sent }

func (m *mockBatch) Rows() int { return m.appendCalls }

func (m *mockBatch) Columns() []chcolumn.Interface { return nil }

func (m *mockBatch) Close() error { return nil }

type mockBatchColumn struct{}

func (m *mockBatchColumn) Append(any) error { return nil }

func (m *mockBatchColumn) AppendRow(any) error { return nil }

type mockReadyRow struct {
	value uint8
	err   error
}

func (m *mockReadyRow) Err() error { return m.err }

func (m *mockReadyRow) Scan(dest ...any) error {
	if m.err != nil {
		return m.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("expected a single destination value")
	}
	target, ok := dest[0].(*uint8)
	if !ok {
		return fmt.Errorf("expected *uint8 destination")
	}
	*target = m.value
	return nil
}

func (m *mockReadyRow) ScanStruct(any) error { return m.err }
