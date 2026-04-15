package providers

import (
	"fmt"
	"net/http"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
	gh "github.com/google/go-github/v83/github"
)

type GitHubHandler struct{}

func (h GitHubHandler) Name() string { return "github" }

func (h GitHubHandler) Handle(r *http.Request, _ []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	if err := requireSecret(h.Name(), cfg); err != nil {
		return WebhookEvent{}, err
	}

	payload, err := gh.ValidatePayload(r, []byte(cfg.Secret))
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("validate github payload: %w", err)
	}

	eventType := r.Header.Get("X-GitHub-Event")
	if eventType == "" {
		return WebhookEvent{}, fmt.Errorf("missing X-GitHub-Event header")
	}

	// Parse the payload when GitHub exposes a typed event, but still allow
	// generic normalization for unknown event shapes.
	_, _ = gh.ParseWebHook(eventType, payload)

	deliveryID := r.Header.Get("X-GitHub-Delivery")
	normEvt, err := norm.NormalizeGitHub(eventType, deliveryID, cfg.TenantID, payload)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("normalize github event: %w", err)
	}
	return WebhookEvent{Normalized: normEvt, DedupID: deliveryID}, nil
}
