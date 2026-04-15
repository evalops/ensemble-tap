package normalize

import (
	"testing"

	tapv1 "github.com/evalops/siphon/proto/tap/v1"
)

func TestTapEventDataFromProtoPreservesNilChanges(t *testing.T) {
	data, err := TapEventDataFromProto(&tapv1.TapEventData{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.Changes != nil {
		t.Fatalf("expected nil changes map, got %#v", data.Changes)
	}
}
