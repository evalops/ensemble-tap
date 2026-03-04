package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/normalize"
	"github.com/nats-io/nats.go"
)

type clickhouseRow struct {
	ID              string
	Type            string
	Source          string
	Subject         string
	Time            time.Time
	Provider        string
	EntityType      string
	EntityID        string
	Action          string
	Changes         string
	Snapshot        string
	ProviderEventID string
	TenantID        *string
}

type ClickHouseSink struct {
	cfg          config.ClickHouseConfig
	natsCfg      config.NATSConfig
	js           nats.JetStreamContext
	metrics      *health.Metrics
	conn         driver.Conn
	insertRowsFn func(context.Context, []clickhouseRow) error
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewClickHouseSink(ctx context.Context, cfg config.ClickHouseConfig, natsCfg config.NATSConfig, js nats.JetStreamContext, metrics *health.Metrics) (*ClickHouseSink, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, nil
	}

	// Connect to default first so schema bootstrap can create cfg.Database when missing.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	sink := &ClickHouseSink{
		cfg:     cfg,
		natsCfg: natsCfg,
		js:      js,
		metrics: metrics,
		conn:    conn,
	}
	if err := sink.initSchema(ctx); err != nil {
		return nil, err
	}
	return sink, nil
}

func (s *ClickHouseSink) initSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s.cfg.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
    id                String,
    type              LowCardinality(String),
    source            LowCardinality(String),
    subject           String,
    time              DateTime64(3),
    provider          LowCardinality(String),
    entity_type       LowCardinality(String),
    entity_id         String,
    action            LowCardinality(String),
    changes           String,
    snapshot          String,
    provider_event_id String,
    tenant_id         Nullable(String),
    received_at       DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (provider, entity_type, time, id)
TTL time + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192`, s.cfg.Database, s.cfg.Table)

	if err := s.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (s *ClickHouseSink) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}

	subject := strings.TrimSuffix(s.natsCfg.SubjectPrefix, ".") + ".>"
	sub, err := s.js.PullSubscribe(
		subject,
		"tap_clickhouse_sink",
		nats.BindStream(s.natsCfg.Stream),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxAckPending(1000),
	)
	if err != nil {
		return fmt.Errorf("create pull subscription for clickhouse sink: %w", err)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.consumeLoop(workerCtx, sub)
	return nil
}

func (s *ClickHouseSink) consumeLoop(ctx context.Context, sub *nats.Subscription) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	rows := make([]clickhouseRow, 0, s.cfg.BatchSize)
	msgs := make([]*nats.Msg, 0, s.cfg.BatchSize)

	flush := func() {
		if len(rows) == 0 {
			return
		}
		batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.insertRows(batchCtx, rows)
		cancel()
		if err != nil {
			if s.metrics != nil {
				s.metrics.EventPublishFailuresTotal.WithLabelValues("clickhouse").Inc()
			}
			for _, msg := range msgs {
				_ = msg.Nak()
			}
			rows = rows[:0]
			msgs = msgs[:0]
			return
		}
		for _, msg := range msgs {
			_ = msg.Ack()
		}
		rows = rows[:0]
		msgs = msgs[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		default:
			fetched, err := sub.Fetch(min(100, s.cfg.BatchSize), nats.MaxWait(500*time.Millisecond))
			if err != nil {
				if err == nats.ErrTimeout {
					continue
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for _, msg := range fetched {
				row, err := eventToRow(msg.Data)
				if err != nil {
					_ = msg.Term()
					continue
				}
				rows = append(rows, row)
				msgs = append(msgs, msg)
				if len(rows) >= s.cfg.BatchSize {
					flush()
				}
			}
		}
	}
}

func (s *ClickHouseSink) insertRows(ctx context.Context, rows []clickhouseRow) error {
	if s.conn == nil {
		if s.insertRowsFn == nil {
			return fmt.Errorf("clickhouse connection is not configured")
		}
		return s.insertRowsFn(ctx, rows)
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.%s (id, type, source, subject, time, provider, entity_type, entity_id, action, changes, snapshot, provider_event_id, tenant_id)",
		s.cfg.Database,
		s.cfg.Table,
	))
	if err != nil {
		return fmt.Errorf("prepare clickhouse batch: %w", err)
	}

	for _, row := range rows {
		if err := batch.Append(
			row.ID,
			row.Type,
			row.Source,
			row.Subject,
			row.Time,
			row.Provider,
			row.EntityType,
			row.EntityID,
			row.Action,
			row.Changes,
			row.Snapshot,
			row.ProviderEventID,
			row.TenantID,
		); err != nil {
			return fmt.Errorf("append clickhouse batch row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send clickhouse batch: %w", err)
	}
	return nil
}

func eventToRow(payload []byte) (clickhouseRow, error) {
	var ce cloudevents.Event
	if err := json.Unmarshal(payload, &ce); err != nil {
		return clickhouseRow{}, fmt.Errorf("decode cloud event: %w", err)
	}
	var data normalize.TapEventData
	if err := ce.DataAs(&data); err != nil {
		return clickhouseRow{}, fmt.Errorf("decode cloud event data: %w", err)
	}

	changesJSON, _ := json.Marshal(data.Changes)
	snapshotJSON, _ := json.Marshal(data.Snapshot)

	var tenantID *string
	if data.TenantID != "" {
		tenantID = &data.TenantID
	}

	when := ce.Time()
	if when.IsZero() {
		when = time.Now().UTC()
	}

	return clickhouseRow{
		ID:              ce.ID(),
		Type:            ce.Type(),
		Source:          ce.Source(),
		Subject:         ce.Subject(),
		Time:            when,
		Provider:        data.Provider,
		EntityType:      data.EntityType,
		EntityID:        data.EntityID,
		Action:          data.Action,
		Changes:         string(changesJSON),
		Snapshot:        string(snapshotJSON),
		ProviderEventID: data.ProviderEventID,
		TenantID:        tenantID,
	}, nil
}

func (s *ClickHouseSink) Close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
