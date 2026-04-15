package providers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

func NormalizeGeneric(provider, eventType, actionHint, providerEventID, tenantID string, body []byte) (normalize.NormalizedEvent, error) {
	var snapshot map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &snapshot)
	}

	entityType, action := deriveEntityAction(eventType, actionHint)
	entityID := extractEntityID(snapshot)

	return normalize.NormalizedEvent{
		Provider:        provider,
		EntityType:      entityType,
		EntityID:        entityID,
		Action:          action,
		ProviderEventID: providerEventID,
		ProviderTime:    extractTimestamp(snapshot),
		TenantID:        tenantID,
		Snapshot:        snapshot,
	}, nil
}

func deriveEntityAction(eventType, actionHint string) (string, string) {
	action := strings.TrimSpace(actionHint)
	if action == "" {
		toks := strings.FieldsFunc(strings.ToLower(eventType), func(r rune) bool {
			return r == '.' || r == '/' || r == ':' || r == '_'
		})
		switch len(toks) {
		case 0:
			action = "updated"
		case 1:
			action = toks[0]
		default:
			action = toks[len(toks)-1]
		}
	}

	toks := strings.FieldsFunc(strings.ToLower(eventType), func(r rune) bool {
		return r == '.' || r == '/' || r == ':' || r == '_'
	})
	entity := "event"
	if len(toks) >= 2 {
		entity = toks[len(toks)-2]
	} else if len(toks) == 1 {
		entity = toks[0]
	}
	return entity, action
}

func extractEntityID(payload map[string]any) string {
	for _, key := range []string{"id", "uuid", "event_id", "object_id", "resource_id"} {
		if v, ok := payload[key]; ok {
			if s := toString(v); s != "" {
				return s
			}
		}
	}

	if data, ok := payload["data"].(map[string]any); ok {
		for _, key := range []string{"id", "object_id"} {
			if v, ok := data[key]; ok {
				if s := toString(v); s != "" {
					return s
				}
			}
		}
	}

	return "unknown"
}

func extractTimestamp(payload map[string]any) time.Time {
	for _, key := range []string{"created", "created_at", "updated_at", "timestamp", "occurred_at", "time"} {
		if v, ok := payload[key]; ok {
			s := toString(v)
			if s == "" {
				continue
			}
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return t.UTC()
			}
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Now().UTC()
}

func toString(v any) string {
	switch vv := v.(type) {
	case string:
		return strings.TrimSpace(vv)
	case json.Number:
		return vv.String()
	case float64:
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(formatFloat(vv)), ".0"), "."))
	case int:
		return strings.TrimSpace(formatInt(int64(vv)))
	case int64:
		return strings.TrimSpace(formatInt(vv))
	default:
		return ""
	}
}

func formatFloat(v float64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func formatInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
