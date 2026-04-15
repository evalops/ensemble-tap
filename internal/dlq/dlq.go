package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/nats-io/nats.go"
)

type Record struct {
	ID              string    `json:"id"`
	Stage           string    `json:"stage"`
	Provider        string    `json:"provider"`
	TenantID        string    `json:"tenant_id,omitempty"`
	RequestID       string    `json:"request_id,omitempty"`
	Reason          string    `json:"reason"`
	OriginalSubject string    `json:"original_subject,omitempty"`
	OriginalDedupID string    `json:"original_dedup_id,omitempty"`
	OriginalPayload []byte    `json:"original_payload,omitempty"`
	OccurredAt      time.Time `json:"occurred_at"`
}

type Republisher func(ctx context.Context, subject string, payload []byte, dedupID string, requestID string) error

type Publisher struct {
	js       nats.JetStreamContext
	natsCfg  config.NATSConfig
	stream   string
	subject  string
	base     string
	consumer string
}

func NewPublisher(ctx context.Context, natsCfg config.NATSConfig, js nats.JetStreamContext) (*Publisher, error) {
	p := &Publisher{
		js:       js,
		natsCfg:  natsCfg,
		stream:   natsCfg.Stream + "_DLQ",
		base:     "siphon.dlq",
		subject:  "siphon.dlq.>",
		consumer: "tap_dlq_replay",
	}
	if err := p.ensureStream(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) ensureStream(_ context.Context) error {
	cfg := &nats.StreamConfig{
		Name:       p.stream,
		Subjects:   []string{p.subject},
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		Storage:    nats.FileStorage,
		MaxAge:     14 * 24 * time.Hour,
		Duplicates: 10 * time.Minute,
	}
	if _, err := p.js.AddStream(cfg); err != nil {
		if _, infoErr := p.js.StreamInfo(p.stream); infoErr != nil {
			return fmt.Errorf("add dlq stream: %w", err)
		}
		if _, err := p.js.UpdateStream(cfg); err != nil {
			return fmt.Errorf("update dlq stream: %w", err)
		}
	}
	return nil
}

func (p *Publisher) Record(ctx context.Context, rec Record) error {
	if strings.TrimSpace(rec.ID) == "" {
		rec.ID = buildID(rec)
	}
	if rec.OccurredAt.IsZero() {
		rec.OccurredAt = time.Now().UTC()
	}
	if strings.TrimSpace(rec.Stage) == "" {
		rec.Stage = "unknown"
	}
	if strings.TrimSpace(rec.Provider) == "" {
		rec.Provider = "unknown"
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	subject := p.base + "." + sanitize(rec.Stage) + "." + sanitize(rec.Provider)
	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, rec.ID)
	_, err = p.js.PublishMsg(msg, nats.Context(ctx))
	return err
}

func (p *Publisher) Replay(ctx context.Context, limit int, republish Republisher) (int, error) {
	if republish == nil {
		return 0, fmt.Errorf("republisher is required")
	}
	if limit <= 0 {
		limit = 100
	}
	sub, err := p.js.PullSubscribe(
		p.subject,
		p.consumer,
		nats.BindStream(p.stream),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxAckPending(1000),
	)
	if err != nil {
		return 0, fmt.Errorf("create dlq replay consumer: %w", err)
	}

	msgs, err := sub.Fetch(limit, nats.MaxWait(2*time.Second))
	if err != nil && err != nats.ErrTimeout {
		return 0, err
	}

	replayed := 0
	for _, msg := range msgs {
		var rec Record
		if err := json.Unmarshal(msg.Data, &rec); err != nil {
			_ = msg.Term()
			continue
		}
		if strings.TrimSpace(rec.OriginalSubject) == "" || len(rec.OriginalPayload) == 0 {
			_ = msg.Term()
			continue
		}
		if err := republish(ctx, rec.OriginalSubject, rec.OriginalPayload, rec.OriginalDedupID, strings.TrimSpace(rec.RequestID)); err != nil {
			_ = msg.Nak()
			return replayed, err
		}
		replayed++
		_ = msg.Ack()
	}
	return replayed, nil
}

func (p *Publisher) Pending() (int, error) {
	if p == nil || p.js == nil {
		return 0, fmt.Errorf("publisher is not initialized")
	}
	consumerInfo, err := p.js.ConsumerInfo(p.stream, p.consumer)
	if err == nil && consumerInfo != nil {
		return safeUint64ToInt(consumerInfo.NumPending)
	}
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		return 0, err
	}
	streamInfo, err := p.js.StreamInfo(p.stream)
	if err != nil {
		return 0, err
	}
	if streamInfo == nil {
		return 0, nil
	}
	return safeUint64ToInt(streamInfo.State.Msgs)
}

func buildID(rec Record) string {
	raw := strings.Join([]string{rec.Stage, rec.Provider, rec.TenantID, rec.RequestID, rec.Reason, rec.OriginalSubject, rec.OriginalDedupID}, "|")
	s := sha256.Sum256([]byte(raw))
	return "dlq_" + hex.EncodeToString(s[:])
}

func sanitize(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "unknown"
	}
	v = strings.ReplaceAll(v, " ", "_")
	v = strings.ReplaceAll(v, "/", "_")
	return v
}

func safeUint64ToInt(value uint64) (int, error) {
	if value > uint64(math.MaxInt) {
		return math.MaxInt, fmt.Errorf("value %d exceeds max int", value)
	}
	return int(value), nil
}
