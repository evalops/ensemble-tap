package poller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/normalize"
)

type SnapshotStore interface {
	Get(provider, entityType, entityID string) (map[string]any, bool)
	Put(provider, entityType, entityID string, snapshot map[string]any) error
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
	Stats          FetchStats
}

type FetchStats struct {
	Requests  int
	Pages     int
	Truncated bool
}

type CycleStats struct {
	FetchedEntities   int
	PublishedEntities int
	SkippedUnchanged  int
	Fetch             FetchStats
}

type Fetcher interface {
	ProviderName() string
	Fetch(ctx context.Context, checkpoint string) (FetchResult, error)
}

func RunCycle(ctx context.Context, fetcher Fetcher, checkpoints CheckpointStore, snapshots SnapshotStore, sink EventSink, tenantID string) error {
	_, err := RunCycleWithStats(ctx, fetcher, checkpoints, snapshots, sink, tenantID)
	return err
}

func RunCycleWithStats(ctx context.Context, fetcher Fetcher, checkpoints CheckpointStore, snapshots SnapshotStore, sink EventSink, tenantID string) (CycleStats, error) {
	stats := CycleStats{}
	if fetcher == nil {
		return stats, fmt.Errorf("fetcher is required")
	}
	if checkpoints == nil {
		return stats, fmt.Errorf("checkpoint store is required")
	}
	if snapshots == nil {
		return stats, fmt.Errorf("snapshot store is required")
	}
	if sink == nil {
		return stats, fmt.Errorf("event sink is required")
	}

	provider := fetcher.ProviderName()
	stateProvider := StateKey(provider, tenantID)
	checkpoint, _ := checkpoints.Get(stateProvider)

	res, err := fetcher.Fetch(ctx, checkpoint)
	if err != nil {
		return stats, err
	}
	stats.FetchedEntities = len(res.Entities)
	stats.Fetch = res.Stats

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

		prev, exists := snapshots.Get(stateProvider, entity.EntityType, entity.EntityID)
		changes := DiffSnapshots(prev, entity.Snapshot)
		action := "updated"
		if !exists {
			action = "created"
		}
		if exists && len(changes) == 0 {
			stats.SkippedUnchanged++
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
		dedup := dedupID(entity, action, tenantID)
		if err := sink.Publish(ctx, evt, dedup); err != nil {
			return stats, fmt.Errorf("publish poll event: %w", err)
		}
		stats.PublishedEntities++
		if err := snapshots.Put(stateProvider, entity.EntityType, entity.EntityID, entity.Snapshot); err != nil {
			return stats, fmt.Errorf("store snapshot: %w", err)
		}
	}

	if res.NextCheckpoint != "" {
		if err := checkpoints.Set(stateProvider, res.NextCheckpoint); err != nil {
			return stats, fmt.Errorf("store checkpoint: %w", err)
		}
	}
	return stats, nil
}

func dedupID(entity Entity, action, tenantID string) string {
	raw := entity.Provider + "|" + tenantID + "|" + entity.EntityType + "|" + entity.EntityID + "|" + action + "|" + entity.UpdatedAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(raw))
	return "poll_" + hex.EncodeToString(sum[:])
}

func StateKey(provider, tenantID string) string {
	provider = strings.TrimSpace(provider)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return provider
	}
	return provider + "::" + tenantID
}
