package publish

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/backoff"
	"github.com/evalops/siphon/internal/health"
	"github.com/evalops/siphon/internal/normalize"
	"github.com/nats-io/nats.go"
)

type NATSPublisher struct {
	cfg     config.NATSConfig
	nc      *nats.Conn
	js      nats.JetStreamContext
	advSub  *nats.Subscription
	metrics *health.Metrics
	ready   atomic.Bool
}

const natsRequestIDHeader = "X-Request-ID"
const (
	defaultNATSConnectTimeout = 5 * time.Second
	defaultNATSReconnectWait  = 2 * time.Second
	defaultNATSMaxReconnects  = -1
	defaultNATSPublishTimeout = 5 * time.Second
	defaultNATSPublishRetries = 3
	defaultNATSPublishBackoff = 100 * time.Millisecond
	defaultNATSReplicas       = 1
	defaultNATSStreamStorage  = "file"
	defaultNATSStreamDiscard  = "old"
	defaultNATSCompression    = "none"
)

func NewNATSPublisher(ctx context.Context, cfg config.NATSConfig, metrics *health.Metrics) (*NATSPublisher, error) {
	cfg = normalizeNATSRuntimeConfig(cfg)
	options := []nats.Option{
		nats.Name("siphon"),
		nats.Timeout(cfg.ConnectTimeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
	}
	options = append(options, natsAuthOptions(cfg)...)
	tlsOptions, err := natsTLSOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure nats tls options: %w", err)
	}
	options = append(options, tlsOptions...)
	nc, err := nats.Connect(cfg.URL, options...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		_ = nc.Drain()
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	p := &NATSPublisher{
		cfg:     cfg,
		nc:      nc,
		js:      js,
		metrics: metrics,
	}

	nc.SetDisconnectErrHandler(func(_ *nats.Conn, _ error) {
		p.ready.Store(false)
		if metrics != nil {
			metrics.NATSConnected.Set(0)
		}
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) {
		p.ready.Store(true)
		if metrics != nil {
			metrics.NATSConnected.Set(1)
		}
	})

	if err := p.ensureStream(ctx); err != nil {
		_ = nc.Drain()
		nc.Close()
		return nil, err
	}
	p.subscribeJetStreamAdvisories()
	p.ready.Store(true)
	if metrics != nil {
		metrics.NATSConnected.Set(1)
	}
	return p, nil
}

func (p *NATSPublisher) ensureStream(ctx context.Context) error {
	subjectPattern := strings.TrimSuffix(p.cfg.SubjectPrefix, ".") + ".>"
	streamCfg := &nats.StreamConfig{
		Name:        p.cfg.Stream,
		Subjects:    []string{subjectPattern},
		Retention:   nats.LimitsPolicy,
		Discard:     streamDiscardPolicy(p.cfg.StreamDiscard),
		Storage:     streamStorageType(p.cfg.StreamStorage),
		MaxAge:      p.cfg.MaxAge,
		Duplicates:  p.cfg.DedupWindow,
		Replicas:    p.cfg.StreamReplicas,
		Compression: streamCompressionType(p.cfg.StreamCompression),
		AllowMsgTTL: p.cfg.StreamAllowMsgTTL,
	}
	if p.cfg.StreamMaxConsumers > 0 {
		streamCfg.MaxConsumers = p.cfg.StreamMaxConsumers
	}
	if p.cfg.StreamMaxMsgsPerSub > 0 {
		streamCfg.MaxMsgsPerSubject = p.cfg.StreamMaxMsgsPerSub
	}
	if p.cfg.StreamMaxMsgs > 0 {
		streamCfg.MaxMsgs = p.cfg.StreamMaxMsgs
	}
	if p.cfg.StreamMaxBytes > 0 {
		streamCfg.MaxBytes = p.cfg.StreamMaxBytes
	}
	if p.cfg.StreamMaxMsgSize > 0 {
		if p.cfg.StreamMaxMsgSize > math.MaxInt32 {
			return fmt.Errorf("nats.stream_max_msg_size %d exceeds max supported %d", p.cfg.StreamMaxMsgSize, math.MaxInt32)
		}
		streamCfg.MaxMsgSize = int32(p.cfg.StreamMaxMsgSize)
	}

	if _, err := p.js.AddStream(streamCfg, nats.Context(ctx)); err == nil {
		return nil
	} else {
		_, infoErr := p.js.StreamInfo(p.cfg.Stream, nats.Context(ctx))
		if infoErr != nil {
			return fmt.Errorf("add stream %s: %w", p.cfg.Stream, err)
		}
		if _, err := p.js.UpdateStream(streamCfg, nats.Context(ctx)); err != nil {
			return fmt.Errorf("update stream %s: %w", p.cfg.Stream, err)
		}
	}
	return nil
}

func (p *NATSPublisher) Publish(ctx context.Context, event cloudevents.Event, dedupID string) (string, error) {
	if !p.ready.Load() {
		return "", fmt.Errorf("nats not ready")
	}
	if err := normalize.ValidateCloudEvent(event); err != nil {
		return "", err
	}

	data, err := normalize.DecodeTapEventData(event)
	if err != nil {
		return "", fmt.Errorf("decode cloudevent data: %w", err)
	}
	subject := normalize.BuildSubjectWithTenant(p.cfg.SubjectPrefix, data.TenantID, data.Provider, data.EntityType, data.Action, p.cfg.TenantScopedSubjects)

	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("marshal cloudevent: %w", err)
	}

	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	if dedupID == "" {
		dedupID = event.ID()
	}
	msg.Header.Set(nats.MsgIdHdr, dedupID)
	requestID := strings.TrimSpace(data.RequestID)
	if requestID == "" {
		requestID = requestIDFromCloudEvent(event)
	}
	if requestID != "" {
		msg.Header.Set(natsRequestIDHeader, requestID)
	}

	pubCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()
	ack, err := p.publishMsgWithRetry(pubCtx, msg)
	if err != nil {
		if p.metrics != nil {
			p.metrics.EventPublishFailuresTotal.WithLabelValues(data.Provider).Inc()
		}
		return subject, fmt.Errorf("publish nats message: %w", err)
	}

	if p.metrics != nil {
		p.metrics.EventsPublishedTotal.WithLabelValues(data.Provider, data.EntityType, data.Action).Inc()
		if ack != nil && ack.Duplicate {
			p.metrics.EventsDedupHitsTotal.WithLabelValues(data.Provider).Inc()
		}
	}
	return subject, nil
}

func (p *NATSPublisher) PublishRaw(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error {
	if !p.ready.Load() {
		return fmt.Errorf("nats not ready")
	}
	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	if strings.TrimSpace(dedupID) != "" {
		msg.Header.Set(nats.MsgIdHdr, dedupID)
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		requestID = requestIDFromCloudEventPayload(payload)
	}
	if requestID != "" {
		msg.Header.Set(natsRequestIDHeader, requestID)
	}
	pubCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()
	_, err := p.publishMsgWithRetry(pubCtx, msg)
	return err
}

func (p *NATSPublisher) JetStream() nats.JetStreamContext {
	return p.js
}

func (p *NATSPublisher) Ready() error {
	if !p.ready.Load() || !p.nc.IsConnected() {
		return fmt.Errorf("nats connection not healthy")
	}
	timeout := p.cfg.ConnectTimeout
	if timeout <= 0 || timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := p.js.AccountInfo(nats.Context(ctx)); err != nil {
		return fmt.Errorf("jetstream api not healthy: %w", err)
	}
	return nil
}

func (p *NATSPublisher) Close() {
	if p.nc == nil {
		return
	}
	if p.advSub != nil {
		_ = p.advSub.Unsubscribe()
		p.advSub = nil
	}
	_ = p.nc.Drain()
	p.nc.Close()
	p.ready.Store(false)
	if p.metrics != nil {
		p.metrics.NATSConnected.Set(0)
	}
}

func (p *NATSPublisher) WaitForClosed(timeout time.Duration) {
	if p.nc == nil {
		return
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	deadline := time.Now().Add(timeout)
	attempt := 0
	for time.Now().Before(deadline) {
		if p.nc.IsClosed() {
			return
		}
		delay := backoff.ExponentialDelay(attempt, 10*time.Millisecond, 250*time.Millisecond)
		attempt++
		if remaining := time.Until(deadline); delay > remaining {
			delay = remaining
		}
		if delay <= 0 {
			return
		}
		time.Sleep(delay)
	}
}

func requestIDFromCloudEvent(event cloudevents.Event) string {
	if ext := event.Extensions(); ext != nil {
		if raw, ok := ext[normalize.TapRequestIDExtension]; ok {
			switch typed := raw.(type) {
			case string:
				return strings.TrimSpace(typed)
			case fmt.Stringer:
				return strings.TrimSpace(typed.String())
			default:
				return strings.TrimSpace(fmt.Sprint(raw))
			}
		}
	}
	data, err := normalize.DecodeTapEventData(event)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(data.RequestID)
}

func requestIDFromCloudEventPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var event cloudevents.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return ""
	}
	return requestIDFromCloudEvent(event)
}

func normalizeNATSRuntimeConfig(cfg config.NATSConfig) config.NATSConfig {
	cfg.Username = strings.TrimSpace(cfg.Username)
	cfg.Password = strings.TrimSpace(cfg.Password)
	cfg.Token = strings.TrimSpace(cfg.Token)
	cfg.CredsFile = strings.TrimSpace(cfg.CredsFile)
	cfg.CAFile = strings.TrimSpace(cfg.CAFile)
	cfg.CertFile = strings.TrimSpace(cfg.CertFile)
	cfg.KeyFile = strings.TrimSpace(cfg.KeyFile)
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultNATSConnectTimeout
	}
	if cfg.ReconnectWait <= 0 {
		cfg.ReconnectWait = defaultNATSReconnectWait
	}
	if cfg.MaxReconnects == 0 {
		cfg.MaxReconnects = defaultNATSMaxReconnects
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = defaultNATSPublishTimeout
	}
	if cfg.PublishMaxRetries <= 0 {
		cfg.PublishMaxRetries = defaultNATSPublishRetries
	}
	if cfg.PublishRetryBackoff <= 0 {
		cfg.PublishRetryBackoff = defaultNATSPublishBackoff
	}
	if cfg.StreamReplicas <= 0 {
		cfg.StreamReplicas = defaultNATSReplicas
	}
	if strings.TrimSpace(cfg.StreamStorage) == "" {
		cfg.StreamStorage = defaultNATSStreamStorage
	}
	if strings.TrimSpace(cfg.StreamDiscard) == "" {
		cfg.StreamDiscard = defaultNATSStreamDiscard
	}
	if strings.TrimSpace(cfg.StreamCompression) == "" {
		cfg.StreamCompression = defaultNATSCompression
	}
	return cfg
}

func streamStorageType(raw string) nats.StorageType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "memory":
		return nats.MemoryStorage
	default:
		return nats.FileStorage
	}
}

func streamDiscardPolicy(raw string) nats.DiscardPolicy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "new":
		return nats.DiscardNew
	default:
		return nats.DiscardOld
	}
}

func streamCompressionType(raw string) nats.StoreCompression {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "s2":
		return nats.S2Compression
	default:
		return nats.NoCompression
	}
}

func natsAuthOptions(cfg config.NATSConfig) []nats.Option {
	if cfg.CredsFile != "" {
		return []nats.Option{nats.UserCredentials(cfg.CredsFile)}
	}
	if cfg.Token != "" {
		return []nats.Option{nats.Token(cfg.Token)}
	}
	if cfg.Username != "" {
		return []nats.Option{nats.UserInfo(cfg.Username, cfg.Password)}
	}
	return nil
}

func natsTLSOptions(cfg config.NATSConfig) ([]nats.Option, error) {
	if !cfg.Secure && !cfg.InsecureSkipVerify && cfg.CAFile == "" && cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.InsecureSkipVerify {
		// #nosec G402 -- operator-controlled setting for trusted/private NATS deployments.
		tlsCfg.InsecureSkipVerify = true
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read nats ca_file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse nats ca_file: no certificates found")
		}
		tlsCfg.RootCAs = roots
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load nats client certificate/key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return []nats.Option{nats.Secure(tlsCfg)}, nil
}

func (p *NATSPublisher) publishMsgWithRetry(ctx context.Context, msg *nats.Msg) (*nats.PubAck, error) {
	maxAttempts := p.cfg.PublishMaxRetries + 1
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		ack, err := p.js.PublishMsg(msg, nats.Context(ctx))
		if err == nil {
			return ack, nil
		}
		lastErr = err
		if attempt == maxAttempts-1 || !shouldRetryNATSPublish(err) {
			break
		}
		if p.metrics != nil {
			p.metrics.NATSPublishRetriesTotal.WithLabelValues(publishRetryReason(err)).Inc()
		}
		delay := backoff.ExponentialDelay(attempt, p.cfg.PublishRetryBackoff, 2*time.Second)
		if p.metrics != nil {
			p.metrics.NATSPublishRetryDelaySeconds.Observe(delay.Seconds())
		}
		if !backoff.SleepContext(ctx, delay) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			break
		}
	}
	return nil, lastErr
}

func shouldRetryNATSPublish(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var apiErr *nats.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code >= 500
	}
	return true
}

func publishRetryReason(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "context"
	}
	var apiErr *nats.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.Code >= 500:
			return "api_5xx"
		case apiErr.Code >= 400:
			return "api_4xx"
		default:
			return "api_other"
		}
	}
	return "transport"
}

func (p *NATSPublisher) subscribeJetStreamAdvisories() {
	if p.nc == nil || p.metrics == nil {
		return
	}
	sub, err := p.nc.Subscribe("$JS.EVENT.ADVISORY.>", func(msg *nats.Msg) {
		kind := advisoryKindFromSubject(msg.Subject)
		p.metrics.JetStreamAdvisoriesTotal.WithLabelValues(kind).Inc()
	})
	if err != nil {
		return
	}
	p.advSub = sub
}

func advisoryKindFromSubject(subject string) string {
	parts := strings.Split(strings.TrimSpace(subject), ".")
	if len(parts) < 5 {
		return "unknown"
	}
	primary := strings.ToLower(strings.TrimSpace(parts[3]))
	secondary := strings.ToLower(strings.TrimSpace(parts[4]))
	if primary == "" {
		return "unknown"
	}
	if secondary == "" {
		return primary
	}
	return primary + "." + secondary
}
