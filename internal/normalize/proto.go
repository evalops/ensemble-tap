package normalize

import (
	"fmt"
	"mime"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	tapv1 "github.com/evalops/ensemble-tap/proto/tap/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const TapProtoContentType = "application/protobuf"

func EncodeTapEventData(data TapEventData) ([]byte, error) {
	msg, err := TapEventDataToProto(data)
	if err != nil {
		return nil, err
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal tap event protobuf: %w", err)
	}
	return payload, nil
}

func DecodeTapEventData(event cloudevents.Event) (TapEventData, error) {
	contentType := normalizeTapContentType(event.DataContentType())
	switch contentType {
	case "", cloudevents.ApplicationJSON:
		var data TapEventData
		if err := event.DataAs(&data); err != nil {
			return TapEventData{}, fmt.Errorf("decode json tap event data: %w", err)
		}
		return data, nil
	case TapProtoContentType:
		return TapEventDataFromProtoBytes(event.Data())
	default:
		var data TapEventData
		if err := event.DataAs(&data); err == nil {
			return data, nil
		}
		return TapEventData{}, fmt.Errorf("unsupported cloud event data content type %q", event.DataContentType())
	}
}

func TapEventDataToProto(data TapEventData) (*tapv1.TapEventData, error) {
	changes := make(map[string]*tapv1.FieldChange, len(data.Changes))
	for key, change := range data.Changes {
		from, err := structpb.NewValue(change.From)
		if err != nil {
			return nil, fmt.Errorf("encode change %q from value: %w", key, err)
		}
		to, err := structpb.NewValue(change.To)
		if err != nil {
			return nil, fmt.Errorf("encode change %q to value: %w", key, err)
		}
		changes[key] = &tapv1.FieldChange{
			From: from,
			To:   to,
		}
	}

	var snapshot *structpb.Struct
	if data.Snapshot != nil {
		var err error
		snapshot, err = structpb.NewStruct(data.Snapshot)
		if err != nil {
			return nil, fmt.Errorf("encode snapshot: %w", err)
		}
	}

	return &tapv1.TapEventData{
		Provider:          data.Provider,
		EntityType:        data.EntityType,
		EntityId:          data.EntityID,
		Action:            data.Action,
		Changes:           changes,
		Snapshot:          snapshot,
		ProviderEventId:   data.ProviderEventID,
		ProviderTimestamp: data.ProviderTimestamp,
		TenantId:          data.TenantID,
		RequestId:         data.RequestID,
	}, nil
}

func TapEventDataFromProtoBytes(payload []byte) (TapEventData, error) {
	if len(payload) == 0 {
		return TapEventData{}, nil
	}
	var msg tapv1.TapEventData
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return TapEventData{}, fmt.Errorf("unmarshal tap event protobuf: %w", err)
	}
	return TapEventDataFromProto(&msg)
}

func TapEventDataFromProto(msg *tapv1.TapEventData) (TapEventData, error) {
	if msg == nil {
		return TapEventData{}, nil
	}

	var changes map[string]FieldChange
	if msg.Changes != nil {
		changes = make(map[string]FieldChange, len(msg.Changes))
		for key, change := range msg.Changes {
			if change == nil {
				changes[key] = FieldChange{}
				continue
			}
			changes[key] = FieldChange{
				From: protoValueToAny(change.GetFrom()),
				To:   protoValueToAny(change.GetTo()),
			}
		}
	}

	var snapshot map[string]any
	if msg.GetSnapshot() != nil {
		snapshot = msg.GetSnapshot().AsMap()
	}

	return TapEventData{
		Provider:          msg.GetProvider(),
		EntityType:        msg.GetEntityType(),
		EntityID:          msg.GetEntityId(),
		Action:            msg.GetAction(),
		Changes:           changes,
		Snapshot:          snapshot,
		ProviderEventID:   msg.GetProviderEventId(),
		ProviderTimestamp: msg.GetProviderTimestamp(),
		TenantID:          msg.GetTenantId(),
		RequestID:         msg.GetRequestId(),
	}, nil
}

func normalizeTapContentType(contentType string) string {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil {
		return strings.ToLower(mediaType)
	}
	return strings.ToLower(contentType)
}

func protoValueToAny(value *structpb.Value) any {
	if value == nil {
		return nil
	}
	return value.AsInterface()
}
