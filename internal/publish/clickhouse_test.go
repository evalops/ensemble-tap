package publish

import (
	"encoding/json"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/internal/normalize"
)

func TestEventToRowMapsCloudEventFields(t *testing.T) {
	when := time.Date(2026, 3, 3, 14, 22, 0, 0, time.UTC)
	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_456",
		ProviderTime:    when,
		TenantID:        "tenant-1",
		Changes: map[string]normalize.FieldChange{
			"status": {From: "open", To: "paid"},
		},
		Snapshot: map[string]any{"amount": 1200},
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	payload, _ := json.Marshal(evt)

	row, err := eventToRow(payload)
	if err != nil {
		t.Fatalf("eventToRow failed: %v", err)
	}

	if row.ID != "evt_456" {
		t.Fatalf("unexpected row ID: %q", row.ID)
	}
	if row.Provider != "stripe" || row.EntityType != "invoice" || row.Action != "paid" {
		t.Fatalf("unexpected row mapping: %+v", row)
	}
	if row.TenantID == nil || *row.TenantID != "tenant-1" {
		t.Fatalf("expected tenant_id to be mapped")
	}
	if row.Time.UTC() != when {
		t.Fatalf("expected row time %s, got %s", when, row.Time)
	}
	if row.Changes == "" || row.Snapshot == "" {
		t.Fatalf("expected JSON fields to be populated")
	}
}

func TestEventToRowUsesCurrentTimeWhenCloudEventTimeMissing(t *testing.T) {
	evt := cloudevents.NewEvent()
	evt.SetSpecVersion(cloudevents.VersionV1)
	evt.SetID("evt_no_time")
	evt.SetType("ensemble.tap.acme.deal.updated")
	evt.SetSource("tap/acme/default")
	evt.SetSubject("deal/1")
	if err := evt.SetData(cloudevents.ApplicationJSON, normalize.TapEventData{
		Provider:   "acme",
		EntityType: "deal",
		EntityID:   "1",
		Action:     "updated",
	}); err != nil {
		t.Fatalf("set event data: %v", err)
	}

	payload, _ := json.Marshal(evt)
	before := time.Now().UTC().Add(-2 * time.Second)
	row, err := eventToRow(payload)
	if err != nil {
		t.Fatalf("eventToRow failed: %v", err)
	}
	if row.Time.Before(before) || row.Time.IsZero() {
		t.Fatalf("expected fallback row time, got %s", row.Time)
	}
	if row.TenantID != nil {
		t.Fatalf("expected nil tenant_id when missing")
	}
}

func TestEventToRowRejectsInvalidPayload(t *testing.T) {
	if _, err := eventToRow([]byte(`not-json`)); err == nil {
		t.Fatalf("expected decode error")
	}
}
