package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/normalize"
	"github.com/nats-io/nats.go"
)

type NATSPublisher struct {
	cfg     config.NATSConfig
	nc      *nats.Conn
	js      nats.JetStreamContext
	metrics *health.Metrics
	ready   atomic.Bool
}

func NewNATSPublisher(ctx context.Context, cfg config.NATSConfig, metrics *health.Metrics) (*NATSPublisher, error) {
	nc, err := nats.Connect(cfg.URL, nats.Name("ensemble-tap"))
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
	p.ready.Store(true)
	if metrics != nil {
		metrics.NATSConnected.Set(1)
	}
	return p, nil
}

func (p *NATSPublisher) ensureStream(_ context.Context) error {
	subjectPattern := strings.TrimSuffix(p.cfg.SubjectPrefix, ".") + ".>"
	streamCfg := &nats.StreamConfig{
		Name:       p.cfg.Stream,
		Subjects:   []string{subjectPattern},
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		Storage:    nats.FileStorage,
		MaxAge:     p.cfg.MaxAge,
		Duplicates: p.cfg.DedupWindow,
	}

	if _, err := p.js.AddStream(streamCfg); err == nil {
		return nil
	} else {
		_, infoErr := p.js.StreamInfo(p.cfg.Stream)
		if infoErr != nil {
			return fmt.Errorf("add stream %s: %w", p.cfg.Stream, err)
		}
		if _, err := p.js.UpdateStream(streamCfg); err != nil {
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

	var data normalize.TapEventData
	if err := event.DataAs(&data); err != nil {
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

	ack, err := p.js.PublishMsg(msg, nats.Context(ctx))
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

func (p *NATSPublisher) PublishRaw(ctx context.Context, subject string, payload []byte, dedupID string) error {
	if !p.ready.Load() {
		return fmt.Errorf("nats not ready")
	}
	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	if strings.TrimSpace(dedupID) != "" {
		msg.Header.Set(nats.MsgIdHdr, dedupID)
	}
	_, err := p.js.PublishMsg(msg, nats.Context(ctx))
	return err
}

func (p *NATSPublisher) JetStream() nats.JetStreamContext {
	return p.js
}

func (p *NATSPublisher) Ready() error {
	if !p.ready.Load() || !p.nc.IsConnected() {
		return fmt.Errorf("nats connection not healthy")
	}
	return nil
}

func (p *NATSPublisher) Close() {
	if p.nc == nil {
		return
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
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.nc.IsClosed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
