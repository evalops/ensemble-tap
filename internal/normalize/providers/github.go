package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

func NormalizeGitHub(eventType, deliveryID, tenantID string, payload []byte) (normalize.NormalizedEvent, error) {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)

	entityType := strings.ToLower(eventType)
	action := "updated"
	if entityType == "push" {
		action = "created"
	} else if v, ok := body["action"].(string); ok && strings.TrimSpace(v) != "" {
		action = strings.ToLower(v)
	}

	entityID := extractEntityID(body)
	if entityID == "unknown" {
		if pr, ok := body["pull_request"].(map[string]any); ok {
			if id := extractEntityID(pr); id != "unknown" {
				entityID = id
			}
		}
	}

	return normalize.NormalizedEvent{
		Provider:        "github",
		EntityType:      entityType,
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: deliveryID,
		ProviderTime:    time.Now().UTC(),
		TenantID:        tenantID,
		Snapshot:        body,
	}, nil
}
