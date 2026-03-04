package normalize

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type FieldChange struct {
	From any `json:"from"`
	To   any `json:"to"`
}

type TapEventData struct {
	Provider          string                 `json:"provider"`
	EntityType        string                 `json:"entity_type"`
	EntityID          string                 `json:"entity_id"`
	Action            string                 `json:"action"`
	Changes           map[string]FieldChange `json:"changes,omitempty"`
	Snapshot          map[string]any         `json:"snapshot,omitempty"`
	ProviderEventID   string                 `json:"provider_event_id,omitempty"`
	ProviderTimestamp string                 `json:"provider_timestamp,omitempty"`
	TenantID          string                 `json:"tenant_id,omitempty"`
}

type NormalizedEvent struct {
	Provider        string
	EntityType      string
	EntityID        string
	Action          string
	ProviderEventID string
	ProviderTime    time.Time
	TenantID        string
	Changes         map[string]FieldChange
	Snapshot        map[string]any
}

func ToCloudEvent(in NormalizedEvent) (cloudevents.Event, error) {
	if err := ValidateNormalizedEvent(in); err != nil {
		return cloudevents.Event{}, err
	}
	if in.Provider == "" {
		return cloudevents.Event{}, fmt.Errorf("provider is required")
	}
	if in.EntityType == "" {
		in.EntityType = "event"
	}
	if in.Action == "" {
		in.Action = "updated"
	}
	if in.EntityID == "" {
		in.EntityID = "unknown"
	}
	if in.ProviderTime.IsZero() {
		in.ProviderTime = time.Now().UTC()
	}

	e := cloudevents.NewEvent()
	e.SetSpecVersion(cloudevents.VersionV1)
	e.SetID(generateEventID(in.ProviderEventID))
	e.SetType(BuildType(in.Provider, in.EntityType, in.Action))
	sourceTenant := strings.TrimSpace(in.TenantID)
	if sourceTenant == "" {
		sourceTenant = "default"
	}
	e.SetSource(fmt.Sprintf("tap/%s/%s", in.Provider, sourceTenant))
	e.SetSubject(fmt.Sprintf("%s/%s", in.EntityType, in.EntityID))
	e.SetTime(in.ProviderTime)
	e.SetExtension("tapversion", TapSchemaVersion)

	data := TapEventData{
		Provider:        in.Provider,
		EntityType:      in.EntityType,
		EntityID:        in.EntityID,
		Action:          in.Action,
		Changes:         in.Changes,
		Snapshot:        in.Snapshot,
		ProviderEventID: in.ProviderEventID,
		TenantID:        in.TenantID,
	}
	if !in.ProviderTime.IsZero() {
		data.ProviderTimestamp = in.ProviderTime.UTC().Format(time.RFC3339Nano)
	}
	if err := e.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return cloudevents.Event{}, fmt.Errorf("set cloudevent data: %w", err)
	}
	if err := ValidateCloudEvent(e); err != nil {
		return cloudevents.Event{}, err
	}
	return e, nil
}

func generateEventID(providerEventID string) string {
	if providerEventID != "" {
		return providerEventID
	}
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}
	return "evt_" + hex.EncodeToString(buf)
}
