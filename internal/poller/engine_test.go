package poller

import (
	"context"
	"testing"
	"time"

	"github.com/evalops/siphon/internal/normalize"
	"github.com/evalops/siphon/internal/store"
)

type fakeFetcher struct {
	provider string
	result   FetchResult
	err      error
}

func (f fakeFetcher) ProviderName() string { return f.provider }

func (f fakeFetcher) Fetch(_ context.Context, _ string) (FetchResult, error) {
	if f.err != nil {
		return FetchResult{}, f.err
	}
	return f.result, nil
}

type capturedEvent struct {
	event normalize.NormalizedEvent
	dedup string
}

type fakeSink struct {
	events []capturedEvent
	err    error
}

func (s *fakeSink) Publish(_ context.Context, event normalize.NormalizedEvent, dedupID string) error {
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, capturedEvent{event: event, dedup: dedupID})
	return nil
}

func TestRunCycleCreatesAndUpdatesEntities(t *testing.T) {
	checkpoints := store.NewInMemoryCheckpointStore()
	snapshots := store.NewInMemorySnapshotStore()
	sink := &fakeSink{}

	createdAt := time.Date(2026, 3, 3, 14, 22, 0, 0, time.UTC)
	fetch := fakeFetcher{
		provider: "hubspot",
		result: FetchResult{
			Entities: []Entity{{
				Provider:   "hubspot",
				EntityType: "deal",
				EntityID:   "d1",
				UpdatedAt:  createdAt,
				Snapshot:   map[string]any{"stage": "open", "amount": 100},
			}},
			NextCheckpoint: createdAt.Format(time.RFC3339Nano),
		},
	}

	if err := RunCycle(context.Background(), fetch, checkpoints, snapshots, sink, "tenant-1"); err != nil {
		t.Fatalf("run cycle create: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(sink.events))
	}
	if sink.events[0].event.Action != "created" {
		t.Fatalf("expected created action, got %q", sink.events[0].event.Action)
	}
	if sink.events[0].dedup == "" {
		t.Fatalf("expected dedup id")
	}
	if cp, ok := checkpoints.Get(StateKey("hubspot", "tenant-1")); !ok || cp == "" {
		t.Fatalf("expected checkpoint to be stored")
	}

	// Same snapshot should emit no new event.
	sink.events = nil
	if err := RunCycle(context.Background(), fetch, checkpoints, snapshots, sink, "tenant-1"); err != nil {
		t.Fatalf("run cycle unchanged: %v", err)
	}
	if len(sink.events) != 0 {
		t.Fatalf("expected no events for unchanged snapshot, got %d", len(sink.events))
	}

	// Updated snapshot should emit updated event with changes.
	updatedAt := createdAt.Add(2 * time.Minute)
	fetch.result.Entities[0].UpdatedAt = updatedAt
	fetch.result.Entities[0].Snapshot = map[string]any{"stage": "won", "amount": 100}
	fetch.result.NextCheckpoint = updatedAt.Format(time.RFC3339Nano)

	if err := RunCycle(context.Background(), fetch, checkpoints, snapshots, sink, "tenant-1"); err != nil {
		t.Fatalf("run cycle update: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 updated event, got %d", len(sink.events))
	}
	if sink.events[0].event.Action != "updated" {
		t.Fatalf("expected updated action, got %q", sink.events[0].event.Action)
	}
	if sink.events[0].event.Changes["stage"].From != "open" || sink.events[0].event.Changes["stage"].To != "won" {
		t.Fatalf("unexpected stage change: %+v", sink.events[0].event.Changes)
	}
}

func TestRunCycleIsolatesTenantState(t *testing.T) {
	checkpoints := store.NewInMemoryCheckpointStore()
	snapshots := store.NewInMemorySnapshotStore()

	fetch := fakeFetcher{
		provider: "hubspot",
		result: FetchResult{
			Entities: []Entity{{
				Provider:   "hubspot",
				EntityType: "deal",
				EntityID:   "d1",
				UpdatedAt:  time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC),
				Snapshot:   map[string]any{"stage": "open"},
			}},
			NextCheckpoint: "2026-03-04T10:00:00Z",
		},
	}

	sinkTenantA := &fakeSink{}
	if err := RunCycle(context.Background(), fetch, checkpoints, snapshots, sinkTenantA, "tenant-a"); err != nil {
		t.Fatalf("run cycle tenant-a: %v", err)
	}
	if len(sinkTenantA.events) != 1 || sinkTenantA.events[0].event.Action != "created" {
		t.Fatalf("expected created event for tenant-a, got %+v", sinkTenantA.events)
	}

	sinkTenantB := &fakeSink{}
	if err := RunCycle(context.Background(), fetch, checkpoints, snapshots, sinkTenantB, "tenant-b"); err != nil {
		t.Fatalf("run cycle tenant-b: %v", err)
	}
	if len(sinkTenantB.events) != 1 || sinkTenantB.events[0].event.Action != "created" {
		t.Fatalf("expected created event for tenant-b, got %+v", sinkTenantB.events)
	}

	if _, ok := checkpoints.Get(StateKey("hubspot", "tenant-a")); !ok {
		t.Fatalf("expected tenant-a checkpoint")
	}
	if _, ok := checkpoints.Get(StateKey("hubspot", "tenant-b")); !ok {
		t.Fatalf("expected tenant-b checkpoint")
	}
}
