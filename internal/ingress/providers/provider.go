package providers

import (
	"fmt"
	"net/http"

	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/normalize"
)

type WebhookEvent struct {
	Normalized normalize.NormalizedEvent
	DedupID    string
}

type Handler interface {
	Name() string
	Handle(r *http.Request, body []byte, cfg config.ProviderConfig) (WebhookEvent, error)
}

func requireSecret(provider string, cfg config.ProviderConfig) error {
	if cfg.Secret == "" {
		return fmt.Errorf("provider %s secret is empty", provider)
	}
	return nil
}
