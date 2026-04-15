package providers

import (
	"fmt"
	"net/http"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
	"github.com/stripe/stripe-go/v82/webhook"
)

type StripeHandler struct{}

func (h StripeHandler) Name() string { return "stripe" }

func (h StripeHandler) Handle(r *http.Request, body []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	if err := requireSecret(h.Name(), cfg); err != nil {
		return WebhookEvent{}, err
	}
	sig := r.Header.Get("Stripe-Signature")
	evt, err := webhook.ConstructEvent(body, sig, cfg.Secret)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("verify stripe webhook: %w", err)
	}
	normEvt, err := norm.NormalizeStripe(evt, cfg.TenantID)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("normalize stripe webhook: %w", err)
	}
	return WebhookEvent{Normalized: normEvt, DedupID: evt.ID}, nil
}
