package store

import (
	"path/filepath"
	"testing"
)

func TestSQLiteStateStoreCheckpointAndSnapshotPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	s, err := NewSQLiteStateStore(path)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}

	if err := s.Checkpoints.Set("hubspot", "cp_1"); err != nil {
		t.Fatalf("set checkpoint: %v", err)
	}
	cp, ok := s.Checkpoints.Get("hubspot")
	if !ok || cp != "cp_1" {
		t.Fatalf("unexpected checkpoint: %q, %v", cp, ok)
	}

	if err := s.Snapshots.Put("hubspot", "deal", "d1", map[string]any{"stage": "open"}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	snap, ok := s.Snapshots.Get("hubspot", "deal", "d1")
	if !ok || snap["stage"] != "open" {
		t.Fatalf("unexpected snapshot: %+v, %v", snap, ok)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	reopen, err := NewSQLiteStateStore(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		_ = reopen.Close()
	}()

	cp2, ok := reopen.Checkpoints.Get("hubspot")
	if !ok || cp2 != "cp_1" {
		t.Fatalf("checkpoint did not persist: %q, %v", cp2, ok)
	}
	snap2, ok := reopen.Snapshots.Get("hubspot", "deal", "d1")
	if !ok || snap2["stage"] != "open" {
		t.Fatalf("snapshot did not persist: %+v, %v", snap2, ok)
	}
}
