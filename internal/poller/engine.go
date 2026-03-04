package poller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/evalops/ensemble-tap/internal/normalize"
)

type SnapshotStore interface {
	Get(provider, entityType, entityID string) (map[string]any, bool)
	Put(provider, entityType, entityID string, snapshot map[string]any)
}

type EventSink interface {
	Publish(ctx context.Context, event normalize.NormalizedEvent, dedupID string) error
}

type Entity struct {
	Provider   string
	EntityType string
	EntityID   string
	Snapshot   map[string]any
	UpdatedAt  time.Time
}

type FetchResult struct {
	Entities       []Entity
	NextCheckpoint string
}

type Fetcher interface {
	ProviderName() string
	Fetch(ctx context.Context, checkpoint string) (FetchResult, error)
}

func RunCycle(ctx context.Context, fetcher Fetcher, checkpoints CheckpointStore, snapshots SnapshotStore, sink EventSink, tenantID string) error {
	if fetcher == nil {
		return fmt.Errorf("fetcher is required")
	}
	if checkpoints == nil {
		return fmt.Errorf("checkpoint store is required")
	}
	if snapshots == nil {
		return fmt.Errorf("snapshot store is required")
	}
	if sink == nil {
		return fmt.Errorf("event sink is required")
	}

	provider := fetcher.ProviderName()
	checkpoint, _ := checkpoints.Get(provider)

	res, err := fetcher.Fetch(ctx, checkpoint)
	if err != nil {
		return err
	}

	for _, entity := range res.Entities {
		if entity.Provider == "" {
			entity.Provider = provider
		}
		if entity.EntityType == "" || entity.EntityID == "" {
			continue
		}
		if entity.Snapshot == nil {
			entity.Snapshot = map[string]any{}
		}
		if entity.UpdatedAt.IsZero() {
			entity.UpdatedAt = time.Now().UTC()
		}

		prev, exists := snapshots.Get(provider, entity.EntityType, entity.EntityID)
		changes := DiffSnapshots(prev, entity.Snapshot)
		action := "updated"
		if !exists {
			action = "created"
		}
		if exists && len(changes) == 0 {
			continue
		}

		evt := normalize.NormalizedEvent{
			Provider:     provider,
			EntityType:   entity.EntityType,
			EntityID:     entity.EntityID,
			Action:       action,
			ProviderTime: entity.UpdatedAt.UTC(),
			TenantID:     tenantID,
			Changes:      changes,
			Snapshot:     entity.Snapshot,
		}
		dedup := dedupID(entity, action)
		if err := sink.Publish(ctx, evt, dedup); err != nil {
			return fmt.Errorf("publish poll event: %w", err)
		}
		snapshots.Put(provider, entity.EntityType, entity.EntityID, entity.Snapshot)
	}

	if res.NextCheckpoint != "" {
		checkpoints.Set(provider, res.NextCheckpoint)
	}
	return nil
}

func dedupID(entity Entity, action string) string {
	raw := entity.Provider + "|" + entity.EntityType + "|" + entity.EntityID + "|" + action + "|" + entity.UpdatedAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(raw))
	return "poll_" + hex.EncodeToString(sum[:])
}
