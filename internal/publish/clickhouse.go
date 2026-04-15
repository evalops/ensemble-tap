package publish

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/backoff"
	"github.com/evalops/siphon/internal/health"
	"github.com/evalops/siphon/internal/normalize"
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
	running      atomic.Bool
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
	defaultClickHouseConsumer   = "tap_clickhouse_sink"
	defaultClickHouseRetention  = 365 * 24 * time.Hour
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
	tlsCfg, err := clickHouseTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure clickhouse tls: %w", err)
	}
	if tlsCfg != nil {
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
		_ = conn.Close()
		return nil, err
	}
	return sink, nil
}

func (s *ClickHouseSink) initSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s.cfg.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	ttlSeconds := int64(s.cfg.RetentionTTL / time.Second)
	if ttlSeconds <= 0 {
		ttlSeconds = int64(defaultClickHouseRetention / time.Second)
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
TTL toDateTime(time) + INTERVAL %d SECOND
SETTINGS index_granularity = 8192`, s.cfg.Database, s.cfg.Table, ttlSeconds)

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
	subOpts := []nats.SubOpt{
		nats.BindStream(s.natsCfg.Stream),
		nats.ManualAck(),
		nats.AckWait(s.cfg.ConsumerAckWait),
		nats.MaxAckPending(s.cfg.ConsumerMaxAckPending),
	}
	if s.cfg.ConsumerMaxDeliver != 0 {
		subOpts = append(subOpts, nats.MaxDeliver(s.cfg.ConsumerMaxDeliver))
	}
	if len(s.cfg.ConsumerBackoff) > 0 {
		subOpts = append(subOpts, nats.BackOff(s.cfg.ConsumerBackoff))
	}
	if s.cfg.ConsumerMaxWaiting > 0 {
		subOpts = append(subOpts, nats.PullMaxWaiting(s.cfg.ConsumerMaxWaiting))
	}
	if s.cfg.ConsumerMaxRequestMaxBytes > 0 {
		subOpts = append(subOpts, nats.MaxRequestMaxBytes(s.cfg.ConsumerMaxRequestMaxBytes))
	}
	sub, err := s.js.PullSubscribe(subject, s.cfg.ConsumerName, subOpts...)
	if err != nil {
		return fmt.Errorf("create pull subscription for clickhouse sink: %w", err)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	s.running.Store(true)
	go s.consumeLoop(workerCtx, sub)
	return nil
}

func (s *ClickHouseSink) consumeLoop(ctx context.Context, sub *nats.Subscription) {
	defer s.wg.Done()
	defer s.running.Store(false)

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
		filteredRows, skipped, err := s.filterDuplicateRows(batchCtx, rows)
		if skipped > 0 && s.metrics != nil {
			s.metrics.ClickHouseDedupSkippedTotal.Add(float64(skipped))
		}
		if err == nil && len(filteredRows) > 0 {
			err = s.insertRows(batchCtx, filteredRows)
		}
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
	data, err := normalize.DecodeTapEventData(ce)
	if err != nil {
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
	id := strings.TrimSpace(ce.ID())
	if id == "" {
		id = strings.TrimSpace(data.ProviderEventID)
	}
	if id == "" {
		id = fallbackEventIDFromPayload(payload)
	}

	return clickhouseRow{
		ID:              id,
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

func (s *ClickHouseSink) Ready() error {
	if s == nil {
		return nil
	}
	if !s.running.Load() {
		return fmt.Errorf("clickhouse sink is not running")
	}
	if s.conn == nil {
		if s.insertRowsFn != nil {
			return nil
		}
		return fmt.Errorf("clickhouse connection is not configured")
	}

	timeout := s.cfg.DialTimeout
	if timeout <= 0 || timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var one uint8
	if err := s.conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("clickhouse health check failed: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("clickhouse health check returned unexpected result %d", one)
	}
	return nil
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
	s.running.Store(false)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func normalizeClickHouseRuntimeConfig(cfg config.ClickHouseConfig) config.ClickHouseConfig {
	cfg.TLSServerName = strings.TrimSpace(cfg.TLSServerName)
	cfg.CAFile = strings.TrimSpace(cfg.CAFile)
	cfg.CertFile = strings.TrimSpace(cfg.CertFile)
	cfg.KeyFile = strings.TrimSpace(cfg.KeyFile)
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
	cfg.ConsumerName = strings.TrimSpace(cfg.ConsumerName)
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = defaultClickHouseConsumer
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
	if len(cfg.ConsumerBackoff) > 0 && cfg.ConsumerMaxDeliver == 0 {
		cfg.ConsumerMaxDeliver = len(cfg.ConsumerBackoff)
	}
	if cfg.ConsumerMaxWaiting < 0 {
		cfg.ConsumerMaxWaiting = 0
	}
	if cfg.ConsumerMaxRequestMaxBytes < 0 {
		cfg.ConsumerMaxRequestMaxBytes = 0
	}
	if cfg.InsertTimeout <= 0 {
		cfg.InsertTimeout = defaultClickHouseInsertTO
	}
	if cfg.RetentionTTL <= 0 {
		cfg.RetentionTTL = defaultClickHouseRetention
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

func clickHouseTLSConfig(cfg config.ClickHouseConfig) (*tls.Config, error) {
	if !cfg.Secure && !cfg.InsecureSkipVerify && cfg.TLSServerName == "" && cfg.CAFile == "" && cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.TLSServerName,
	}
	if cfg.InsecureSkipVerify {
		// #nosec G402 -- operator-controlled setting for private/internal ClickHouse deployments.
		tlsCfg.InsecureSkipVerify = true
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read clickhouse ca_file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse clickhouse ca_file: no certificates found")
		}
		tlsCfg.RootCAs = roots
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load clickhouse client certificate/key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func (s *ClickHouseSink) filterDuplicateRows(ctx context.Context, rows []clickhouseRow) ([]clickhouseRow, int, error) {
	if len(rows) == 0 {
		return nil, 0, nil
	}

	seenInBatch := make(map[string]struct{}, len(rows))
	filtered := make([]clickhouseRow, 0, len(rows))
	keys := make([]string, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		key := strings.TrimSpace(row.ID)
		if key == "" {
			filtered = append(filtered, row)
			continue
		}
		if _, exists := seenInBatch[key]; exists {
			skipped++
			continue
		}
		seenInBatch[key] = struct{}{}
		keys = append(keys, key)
		filtered = append(filtered, row)
	}

	if s.conn == nil || len(keys) == 0 {
		return filtered, skipped, nil
	}

	existingKeys, err := s.loadExistingRowIDs(ctx, keys)
	if err != nil {
		return nil, skipped, err
	}
	if len(existingKeys) == 0 {
		return filtered, skipped, nil
	}

	deduped := make([]clickhouseRow, 0, len(filtered))
	for _, row := range filtered {
		key := strings.TrimSpace(row.ID)
		if key != "" {
			if _, exists := existingKeys[key]; exists {
				skipped++
				continue
			}
		}
		deduped = append(deduped, row)
	}
	return deduped, skipped, nil
}

func (s *ClickHouseSink) loadExistingRowIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	existing := make(map[string]struct{})
	for _, chunk := range chunkStrings(ids, 200) {
		if len(chunk) == 0 {
			continue
		}
		query := fmt.Sprintf(
			"SELECT id FROM %s.%s WHERE id IN (%s)",
			s.cfg.Database,
			s.cfg.Table,
			quotedStringList(chunk),
		)
		rows, err := s.conn.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("query existing clickhouse ids: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan existing clickhouse id: %w", err)
			}
			id = strings.TrimSpace(id)
			if id != "" {
				existing[id] = struct{}{}
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate existing clickhouse ids: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close existing clickhouse id rows: %w", err)
		}
	}
	return existing, nil
}

func quotedStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		quoted = append(quoted, "'"+strings.ReplaceAll(v, "'", "''")+"'")
	}
	if len(quoted) == 0 {
		return "''"
	}
	return strings.Join(quoted, ",")
}

func chunkStrings(values []string, chunkSize int) [][]string {
	if chunkSize <= 0 || len(values) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(values)+chunkSize-1)/chunkSize)
	for start := 0; start < len(values); start += chunkSize {
		end := start + chunkSize
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func fallbackEventIDFromPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "evt_" + hex.EncodeToString(sum[:])
}
