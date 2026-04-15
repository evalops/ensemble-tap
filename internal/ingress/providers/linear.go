package providers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
)

type LinearHandler struct{}

func (h LinearHandler) Name() string { return "linear" }

func (h LinearHandler) Handle(r *http.Request, body []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	if err := requireSecret(h.Name(), cfg); err != nil {
		return WebhookEvent{}, err
	}
	verifier := ProviderVerifier{
		Algorithm:   "sha256",
		SigHeader:   "Linear-Signature",
		SigEncoding: "hex",
	}
	if err := verifier.Verify(r, body, cfg.Secret); err != nil {
		return WebhookEvent{}, fmt.Errorf("verify linear webhook: %w", err)
	}

	if err := verifyLinearReplayWindow(body, 60*time.Second); err != nil {
		return WebhookEvent{}, err
	}

	eventName := r.Header.Get("Linear-Event")
	deliveryID := r.Header.Get("Linear-Delivery")
	normEvt, err := norm.NormalizeLinear(eventName, deliveryID, cfg.TenantID, body)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("normalize linear event: %w", err)
	}
	return WebhookEvent{Normalized: normEvt, DedupID: deliveryID}, nil
}

func verifyLinearReplayWindow(body []byte, maxAge time.Duration) error {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	ts, ok := payload["webhookTimestamp"]
	if !ok {
		return nil
	}

	var t time.Time
	switch v := ts.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			t = parsed
		}
	case float64:
		t = time.Unix(int64(v), 0)
	}
	if t.IsZero() {
		return nil
	}
	if age := time.Since(t); age > maxAge || age < -maxAge {
		return fmt.Errorf("linear webhook timestamp outside replay window")
	}
	return nil
}
