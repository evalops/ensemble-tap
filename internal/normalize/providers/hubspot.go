package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

func NormalizeHubSpot(providerEventID, tenantID string, payload []byte) (normalize.NormalizedEvent, error) {
	var arr []map[string]any
	if err := json.Unmarshal(payload, &arr); err != nil || len(arr) == 0 {
		arr = []map[string]any{{}}
		_ = json.Unmarshal(payload, &arr[0])
	}

	e := arr[0]
	subscriptionType, _ := e["subscriptionType"].(string)
	objectType, _ := e["objectType"].(string)
	if objectType == "" {
		objectType = "object"
	}
	action := "updated"
	if strings.Contains(strings.ToLower(subscriptionType), ".creation") {
		action = "created"
	} else if strings.Contains(strings.ToLower(subscriptionType), ".deletion") {
		action = "deleted"
	}

	entityID := toString(e["objectId"])
	if entityID == "" {
		entityID = "unknown"
	}

	changes := map[string]normalize.FieldChange{}
	if propertyName := toString(e["propertyName"]); propertyName != "" {
		changes[propertyName] = normalize.FieldChange{From: nil, To: e["propertyValue"]}
	}

	pt := time.Now().UTC()
	if occurredAt := toString(e["occurredAt"]); occurredAt != "" {
		if ts, err := time.Parse(time.RFC3339, occurredAt); err == nil {
			pt = ts.UTC()
		}
	}

	return normalize.NormalizedEvent{
		Provider:        "hubspot",
		EntityType:      strings.ToLower(objectType),
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: providerEventID,
		ProviderTime:    pt,
		TenantID:        tenantID,
		Changes:         changes,
		Snapshot:        e,
	}, nil
}
