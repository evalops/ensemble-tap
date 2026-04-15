package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
	"github.com/stripe/stripe-go/v82"
)

func NormalizeStripe(evt stripe.Event, tenantID string) (normalize.NormalizedEvent, error) {
	eventType := string(evt.Type)
	parts := strings.Split(eventType, ".")
	entityType := "event"
	action := "updated"
	if len(parts) >= 1 {
		entityType = parts[0]
	}
	if len(parts) >= 2 {
		action = parts[len(parts)-1]
	}

	snapshot := map[string]any{}
	if evt.Data != nil {
		if len(evt.Data.Raw) > 0 {
			_ = json.Unmarshal(evt.Data.Raw, &snapshot)
		}
	}

	entityID := extractEntityID(snapshot)
	if entityID == "unknown" && evt.Data != nil && evt.Data.Object != nil {
		obj := map[string]any{}
		raw, _ := json.Marshal(evt.Data.Object)
		_ = json.Unmarshal(raw, &obj)
		if id := extractEntityID(obj); id != "unknown" {
			entityID = id
		}
		if len(snapshot) == 0 {
			snapshot = obj
		}
	}

	pt := time.Now().UTC()
	if evt.Created > 0 {
		pt = time.Unix(evt.Created, 0).UTC()
	}

	return normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      entityType,
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: evt.ID,
		ProviderTime:    pt,
		TenantID:        tenantID,
		Snapshot:        snapshot,
	}, nil
}
