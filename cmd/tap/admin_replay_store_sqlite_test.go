package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAdminReplayJobSQLiteStoreRequiresPath(t *testing.T) {
	if _, err := newAdminReplayJobSQLiteStore("   "); err == nil {
		t.Fatalf("expected sqlite replay store path validation error")
	}
}

func TestAdminReplayJobSQLiteStoreRoundTrip(t *testing.T) {
	store, err := newAdminReplayJobSQLiteStore(filepath.Join(t.TempDir(), "replay-jobs.db"))
	if err != nil {
		t.Fatalf("create sqlite replay store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite replay store: %v", err)
		}
	}()

	createdAt := time.Now().UTC().Add(-2 * time.Minute).Round(0)
	startedAt := createdAt.Add(2 * time.Second).Round(0)
	completedAt := startedAt.Add(3 * time.Second).Round(0)
	updatedAt := completedAt.Add(2 * time.Second).Round(0)
	job := &adminReplayJob{
		snapshot: adminReplayJobSnapshot{
			JobID:          "replay_1730800000000000000_1",
			Status:         adminReplayJobStatusCancelled,
			RequestedLimit: 10,
			EffectiveLimit: 5,
			MaxLimit:       5,
			Capped:         true,
			DryRun:         true,
			CreatedAt:      createdAt,
			StartedAt:      startedAt,
			CompletedAt:    completedAt,
			Replayed:       0,
			OperatorReason: "manual replay safety check",
			CancelReason:   "operator aborted request",
			Error:          "cancelled by operator: operator aborted request",
		},
		idempotencyKey:          "sqlite-store-idem-1",
		creatorIP:               "203.0.113.50",
		creatorTokenFingerprint: "token-fp-1234",
		updatedAt:               updatedAt,
	}
	if err := store.Upsert(job); err != nil {
		t.Fatalf("upsert replay job: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load replay jobs: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected one replay job after upsert, got %d", len(loaded))
	}
	got := loaded[0]
	if got == nil {
		t.Fatalf("expected non-nil loaded replay job")
	}
	if got.snapshot.JobID != job.snapshot.JobID || got.snapshot.Status != adminReplayJobStatusCancelled {
		t.Fatalf("unexpected loaded replay job identity/status: %+v", got.snapshot)
	}
	if !got.snapshot.CreatedAt.Equal(createdAt) || !got.snapshot.StartedAt.Equal(startedAt) || !got.snapshot.CompletedAt.Equal(completedAt) {
		t.Fatalf("unexpected loaded replay timestamps: created=%s started=%s completed=%s", got.snapshot.CreatedAt, got.snapshot.StartedAt, got.snapshot.CompletedAt)
	}
	if got.snapshot.OperatorReason != job.snapshot.OperatorReason || got.snapshot.CancelReason != job.snapshot.CancelReason {
		t.Fatalf("unexpected loaded replay reasons: operator=%q cancel=%q", got.snapshot.OperatorReason, got.snapshot.CancelReason)
	}
	if got.idempotencyKey != job.idempotencyKey || got.creatorIP != job.creatorIP || got.creatorTokenFingerprint != job.creatorTokenFingerprint {
		t.Fatalf("unexpected loaded replay job metadata: idem=%q ip=%q token=%q", got.idempotencyKey, got.creatorIP, got.creatorTokenFingerprint)
	}
	if !got.updatedAt.Equal(updatedAt) {
		t.Fatalf("unexpected loaded updated_at: %s", got.updatedAt)
	}

	job.snapshot.Status = adminReplayJobStatusSucceeded
	job.snapshot.CancelReason = ""
	job.snapshot.Error = ""
	job.snapshot.Replayed = 5
	job.snapshot.CompletedAt = time.Now().UTC().Round(0)
	job.updatedAt = job.snapshot.CompletedAt.Add(time.Second).Round(0)
	if err := store.Upsert(job); err != nil {
		t.Fatalf("update replay job: %v", err)
	}
	loaded, err = store.Load()
	if err != nil {
		t.Fatalf("reload replay jobs after update: %v", err)
	}
	if len(loaded) != 1 || loaded[0].snapshot.Status != adminReplayJobStatusSucceeded || loaded[0].snapshot.Replayed != 5 {
		t.Fatalf("expected replay job update to persist, got %+v", loaded)
	}
	if loaded[0].snapshot.CancelReason != "" {
		t.Fatalf("expected cancel_reason to be cleared after update, got %q", loaded[0].snapshot.CancelReason)
	}

	if err := store.Delete(job.snapshot.JobID); err != nil {
		t.Fatalf("delete replay job: %v", err)
	}
	loaded, err = store.Load()
	if err != nil {
		t.Fatalf("load replay jobs after delete: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("expected no replay jobs after delete, got %d", len(loaded))
	}
}

func TestAdminReplayJobSQLiteStoreUpsertNilNoop(t *testing.T) {
	store, err := newAdminReplayJobSQLiteStore(filepath.Join(t.TempDir(), "replay-jobs.db"))
	if err != nil {
		t.Fatalf("create sqlite replay store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite replay store: %v", err)
		}
	}()
	if err := store.Upsert(nil); err != nil {
		t.Fatalf("expected nil replay job upsert to be a noop, got error: %v", err)
	}
}
