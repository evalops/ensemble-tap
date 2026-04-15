package normalize

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func TestValidateNormalizedEvent(t *testing.T) {
	if err := ValidateNormalizedEvent(NormalizedEvent{}); err == nil {
		t.Fatalf("expected validation error for empty event")
	}
	if err := ValidateNormalizedEvent(NormalizedEvent{Provider: "hubspot", EntityType: "deal", EntityID: "1", Action: "updated"}); err != nil {
		t.Fatalf("expected normalized event to validate: %v", err)
	}
}

func TestValidateCloudEvent(t *testing.T) {
	e := cloudevents.NewEvent()
	e.SetSpecVersion(cloudevents.VersionV1)
	e.SetID("evt_1")
	e.SetType("siphon.tap.hubspot.deal.updated")
	e.SetSource("tap/hubspot/default")
	if err := e.SetData(cloudevents.ApplicationJSON, TapEventData{Provider: "hubspot", EntityType: "deal", EntityID: "1", Action: "updated"}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := ValidateCloudEvent(e); err != nil {
		t.Fatalf("expected valid cloudevent: %v", err)
	}

	invalid := cloudevents.NewEvent()
	if err := ValidateCloudEvent(invalid); err == nil {
		t.Fatalf("expected invalid cloud event to fail")
	}
}

func TestDecodeTapEventDataSupportsLegacyJSON(t *testing.T) {
	e := cloudevents.NewEvent()
	e.SetSpecVersion(cloudevents.VersionV1)
	e.SetID("evt_json")
	e.SetType("siphon.tap.hubspot.deal.updated")
	e.SetSource("tap/hubspot/default")
	if err := e.SetData(cloudevents.ApplicationJSON, TapEventData{
		Provider:   "hubspot",
		EntityType: "deal",
		EntityID:   "1",
		Action:     "updated",
		RequestID:  "req-json-1",
	}); err != nil {
		t.Fatalf("set legacy json data: %v", err)
	}

	data, err := DecodeTapEventData(e)
	if err != nil {
		t.Fatalf("decode legacy json data: %v", err)
	}
	if data.RequestID != "req-json-1" {
		t.Fatalf("expected legacy request id to round-trip, got %q", data.RequestID)
	}
}
