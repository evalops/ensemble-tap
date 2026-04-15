package normalize

import (
	"testing"
	"time"
)

func TestToCloudEvent(t *testing.T) {
	now := time.Date(2026, 3, 3, 14, 22, 0, 0, time.UTC)
	evt, err := ToCloudEvent(NormalizedEvent{
		Provider:        "hubspot",
		EntityType:      "deal",
		EntityID:        "12345",
		Action:          "updated",
		ProviderEventID: "evt_123",
		ProviderTime:    now,
		TenantID:        "workspace-123",
		Snapshot: map[string]any{
			"dealname": "Acme",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if evt.Type() != "siphon.tap.hubspot.deal.updated" {
		t.Fatalf("unexpected type: %s", evt.Type())
	}
	if evt.Source() != "tap/hubspot/workspace-123" {
		t.Fatalf("unexpected source: %s", evt.Source())
	}
	if evt.Subject() != "deal/12345" {
		t.Fatalf("unexpected subject: %s", evt.Subject())
	}
	if evt.DataContentType() != TapProtoContentType {
		t.Fatalf("unexpected data content type: %s", evt.DataContentType())
	}

	data, err := DecodeTapEventData(evt)
	if err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data.Provider != "hubspot" || data.EntityType != "deal" || data.Action != "updated" {
		t.Fatalf("unexpected data: %+v", data)
	}
}
