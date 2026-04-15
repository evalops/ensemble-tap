package providers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	cfgpkg "github.com/evalops/siphon/config"
	norm "github.com/evalops/siphon/internal/normalize/providers"
)

type HubSpotHandler struct{}

func (h HubSpotHandler) Name() string { return "hubspot" }

func (h HubSpotHandler) Handle(r *http.Request, body []byte, cfg cfgpkg.ProviderConfig) (WebhookEvent, error) {
	secret := cfg.ClientSecret
	if secret == "" {
		secret = cfg.Secret
	}
	if secret == "" {
		return WebhookEvent{}, fmt.Errorf("provider hubspot secret/client_secret is empty")
	}

	verifier := ProviderVerifier{
		Algorithm:       "sha256",
		SigHeader:       "X-HubSpot-Signature-v3",
		SigEncoding:     "base64",
		TimestampHeader: "X-HubSpot-Request-Timestamp",
		MaxAge:          5 * time.Minute,
		BuildSignedPayload: func(r *http.Request, body []byte) ([]byte, error) {
			ts := strings.TrimSpace(r.Header.Get("X-HubSpot-Request-Timestamp"))
			baseURL := resolveRequestURL(r)
			return []byte(r.Method + baseURL + string(body) + ts), nil
		},
	}
	if err := verifier.Verify(r, body, secret); err != nil {
		return WebhookEvent{}, fmt.Errorf("verify hubspot webhook: %w", err)
	}

	providerEventID := r.Header.Get("X-HubSpot-Event-Id")
	normEvt, err := norm.NormalizeHubSpot(providerEventID, cfg.TenantID, body)
	if err != nil {
		return WebhookEvent{}, fmt.Errorf("normalize hubspot event: %w", err)
	}
	return WebhookEvent{Normalized: normEvt, DedupID: providerEventID}, nil
}

func resolveRequestURL(r *http.Request) string {
	scheme := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		scheme = "https"
	}
	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host + r.URL.RequestURI()
}
