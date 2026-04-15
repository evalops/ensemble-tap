package providers

import (
	"fmt"
	"net/http"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
)

type GenericHandler struct{}

func (h GenericHandler) Name() string { return "generic" }

func (h GenericHandler) Handle(r *http.Request, body []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	if err := requireSecret(h.Name(), cfg); err != nil {
		return WebhookEvent{}, err
	}
	verifier := ProviderVerifier{
		Algorithm:   "sha256",
		SigHeader:   "X-Signature",
		SigEncoding: "prefixed-hex",
		SigPrefix:   "sha256=",
	}
	if err := verifier.Verify(r, body, cfg.Secret); err != nil {
		return WebhookEvent{}, fmt.Errorf("verify generic webhook: %w", err)
	}

	eventType := r.Header.Get("X-Event-Type")
	action := r.Header.Get("X-Event-Action")
	eventID := r.Header.Get("X-Event-Id")
	provider := r.PathValue("provider")
	normEvt, err := norm.NormalizeGeneric(provider, eventType, action, eventID, cfg.TenantID, body)
	if err != nil {
		return WebhookEvent{}, err
	}
	return WebhookEvent{Normalized: normEvt, DedupID: eventID}, nil
}
