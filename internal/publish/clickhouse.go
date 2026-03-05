package publish

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/backoff"
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

const (
	defaultClickHouseUser       = "default"
	defaultClickHouseDialTO     = 5 * time.Second
	defaultClickHouseMaxOpen    = 4
	defaultClickHouseMaxIdle    = 2
	defaultClickHouseConnMaxAge = 30 * time.Minute
	defaultClickHouseBatchSize  = 500
	defaultClickHouseFlushEvery = 2 * time.Second
	defaultClickHouseFetchBatch = 100
	defaultClickHouseFetchWait  = 500 * time.Millisecond
	defaultClickHouseAckWait    = 30 * time.Second
	defaultClickHouseAckPending = 1000
	defaultClickHouseInsertTO   = 10 * time.Second
)

func NewClickHouseSink(ctx context.Context, cfg config.ClickHouseConfig, natsCfg config.NATSConfig, js nats.JetStreamContext, metrics *health.Metrics) (*ClickHouseSink, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, nil
	}
	cfg = normalizeClickHouseRuntimeConfig(cfg)

	// Connect to default first so schema bootstrap can create cfg.Database when missing.
	opts := &clickhouse.Options{
		Addr:            clickHouseAddresses(cfg.Addr),
		Auth:            clickhouse.Auth{Database: "default", Username: cfg.Username, Password: cfg.Password},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
	}
	if cfg.Secure || cfg.InsecureSkipVerify {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.InsecureSkipVerify {
			// #nosec G402 -- operator-controlled setting for private/internal ClickHouse deployments.
			tlsCfg.InsecureSkipVerify = true
		}
		opts.TLS = tlsCfg
	}
	conn, err := clickhouse.Open(opts)
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
TTL toDateTime(time) + INTERVAL 1 YEAR
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
	s.cfg = normalizeClickHouseRuntimeConfig(s.cfg)

	subject := strings.TrimSuffix(s.natsCfg.SubjectPrefix, ".") + ".>"
	sub, err := s.js.PullSubscribe(
		subject,
		"tap_clickhouse_sink",
		nats.BindStream(s.natsCfg.Stream),
		nats.ManualAck(),
		nats.AckWait(s.cfg.ConsumerAckWait),
		nats.MaxAckPending(s.cfg.ConsumerMaxAckPending),
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
	fetchErrStreak := 0

	flush := func() {
		if len(rows) == 0 {
			return
		}
		batchCtx, cancel := context.WithTimeout(ctx, s.cfg.InsertTimeout)
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
			fetchBatch := min(s.cfg.ConsumerFetchBatch, s.cfg.BatchSize)
			if fetchBatch <= 0 {
				fetchBatch = 1
			}
			fetched, err := sub.Fetch(fetchBatch, nats.MaxWait(s.cfg.ConsumerFetchMaxWait))
			if err != nil {
				if err == nats.ErrTimeout {
					fetchErrStreak = 0
					continue
				}
				fetchErrStreak++
				delay := backoff.ExponentialDelay(fetchErrStreak-1, 100*time.Millisecond, 2*time.Second)
				if !backoff.SleepContext(ctx, delay) {
					flush()
					return
				}
				continue
			}
			fetchErrStreak = 0
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

func normalizeClickHouseRuntimeConfig(cfg config.ClickHouseConfig) config.ClickHouseConfig {
	if strings.TrimSpace(cfg.Username) == "" {
		cfg.Username = defaultClickHouseUser
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultClickHouseDialTO
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = defaultClickHouseMaxOpen
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultClickHouseMaxIdle
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		cfg.MaxIdleConns = cfg.MaxOpenConns
	}
	if cfg.ConnMaxLifetime < 0 {
		cfg.ConnMaxLifetime = defaultClickHouseConnMaxAge
	}
	if cfg.ConnMaxLifetime == 0 {
		cfg.ConnMaxLifetime = defaultClickHouseConnMaxAge
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultClickHouseBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultClickHouseFlushEvery
	}
	if cfg.ConsumerFetchBatch <= 0 {
		cfg.ConsumerFetchBatch = defaultClickHouseFetchBatch
	}
	if cfg.ConsumerFetchMaxWait <= 0 {
		cfg.ConsumerFetchMaxWait = defaultClickHouseFetchWait
	}
	if cfg.ConsumerAckWait <= 0 {
		cfg.ConsumerAckWait = defaultClickHouseAckWait
	}
	if cfg.ConsumerMaxAckPending <= 0 {
		cfg.ConsumerMaxAckPending = defaultClickHouseAckPending
	}
	if cfg.InsertTimeout <= 0 {
		cfg.InsertTimeout = defaultClickHouseInsertTO
	}
	return cfg
}

func clickHouseAddresses(raw string) []string {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		out = append(out, addr)
	}
	if len(out) == 0 && strings.TrimSpace(raw) != "" {
		return []string{strings.TrimSpace(raw)}
	}
	return out
}
