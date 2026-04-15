package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

func NormalizeShopify(topic, providerEventID, tenantID string, payload []byte) (normalize.NormalizedEvent, error) {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)

	toks := strings.Split(strings.ToLower(topic), "/")
	entityType := "event"
	action := "updated"
	if len(toks) >= 1 {
		entityType = toks[0]
	}
	if len(toks) >= 2 {
		action = toks[1]
	}

	entityID := extractEntityID(body)
	if entityID == "unknown" {
		entityID = toString(body["admin_graphql_api_id"])
		if entityID == "" {
			entityID = "unknown"
		}
	}

	return normalize.NormalizedEvent{
		Provider:        "shopify",
		EntityType:      entityType,
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: providerEventID,
		ProviderTime:    time.Now().UTC(),
		TenantID:        tenantID,
		Snapshot:        body,
	}, nil
}
