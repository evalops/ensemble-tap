package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

func NormalizeLinear(eventName, deliveryID, tenantID string, payload []byte) (normalize.NormalizedEvent, error) {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)

	action := strings.ToLower(eventName)
	entityType := "event"
	if bodyType, ok := body["type"].(string); ok && strings.TrimSpace(bodyType) != "" {
		entityType = strings.ToLower(bodyType)
	}
	if strings.Contains(action, ".") {
		parts := strings.Split(action, ".")
		if len(parts) >= 2 {
			entityType = parts[0]
			action = parts[1]
		}
	}

	snapshot := body
	if data, ok := body["data"].(map[string]any); ok {
		snapshot = data
	}

	entityID := extractEntityID(snapshot)
	if entityID == "unknown" {
		entityID = extractEntityID(body)
	}

	pt := time.Now().UTC()
	if tsRaw, ok := body["webhookTimestamp"]; ok {
		if ts := toString(tsRaw); ts != "" {
			if ms, err := time.Parse(time.RFC3339, ts); err == nil {
				pt = ms.UTC()
			}
		}
	}

	return normalize.NormalizedEvent{
		Provider:        "linear",
		EntityType:      entityType,
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: deliveryID,
		ProviderTime:    pt,
		TenantID:        tenantID,
		Snapshot:        body,
	}, nil
}
