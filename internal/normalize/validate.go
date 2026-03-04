package normalize

import (
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

const TapSchemaVersion = "v1"

func ValidateNormalizedEvent(in NormalizedEvent) error {
	if strings.TrimSpace(in.Provider) == "" {
		return fmt.Errorf("provider is required")
	}
	if strings.TrimSpace(in.EntityType) == "" {
		return fmt.Errorf("entity_type is required")
	}
	if strings.TrimSpace(in.EntityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(in.Action) == "" {
		return fmt.Errorf("action is required")
	}
	return nil
}

func ValidateCloudEvent(event cloudevents.Event) error {
	if strings.TrimSpace(event.SpecVersion()) == "" {
		return fmt.Errorf("specversion is required")
	}
	if strings.TrimSpace(event.ID()) == "" {
		return fmt.Errorf("id is required")
	}
	if strings.TrimSpace(event.Type()) == "" {
		return fmt.Errorf("type is required")
	}
	if strings.TrimSpace(event.Source()) == "" {
		return fmt.Errorf("source is required")
	}

	var data TapEventData
	if err := event.DataAs(&data); err != nil {
		return fmt.Errorf("invalid data payload: %w", err)
	}
	if strings.TrimSpace(data.Provider) == "" {
		return fmt.Errorf("data.provider is required")
	}
	if strings.TrimSpace(data.EntityType) == "" {
		return fmt.Errorf("data.entity_type is required")
	}
	if strings.TrimSpace(data.EntityID) == "" {
		return fmt.Errorf("data.entity_id is required")
	}
	if strings.TrimSpace(data.Action) == "" {
		return fmt.Errorf("data.action is required")
	}
	return nil
}
