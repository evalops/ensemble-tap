package providers

import (
	"fmt"
	"net/http"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
)

type ShopifyHandler struct{}

func (h ShopifyHandler) Name() string { return "shopify" }

func (h ShopifyHandler) Handle(r *http.Request, body []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	if err := requireSecret(h.Name(), cfg); err != nil {
		return WebhookEvent{}, err
	}
	verifier := ProviderVerifier{
		Algorithm:   "sha256",
		SigHeader:   "X-Shopify-Hmac-SHA256",
		SigEncoding: "base64",
	}
	if err := verifier.Verify(r, body, cfg.Secret); err != nil {
		return WebhookEvent{}, fmt.Errorf("verify shopify webhook: %w", err)
	}
	topic := r.Header.Get("X-Shopify-Topic")
	if topic == "" {
		topic = "event.updated"
	}
	eventID := r.Header.Get("X-Shopify-Event-Id")
	normEvt, err := norm.NormalizeShopify(topic, eventID, cfg.TenantID, body)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("normalize shopify event: %w", err)
	}
	return WebhookEvent{Normalized: normEvt, DedupID: eventID}, nil
}
