package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type SQLiteStateStore struct {
	db          *sql.DB
	Checkpoints *SQLiteCheckpointStore
	Snapshots   *SQLiteSnapshotStore
}

type SQLiteCheckpointStore struct {
	db *sql.DB
}

type SQLiteSnapshotStore struct {
	db *sql.DB
}

func NewSQLiteStateStore(path string) (*SQLiteStateStore, error) {
	if path == "" {
		path = "tap-state.db"
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	store := &SQLiteStateStore{
		db:          db,
		Checkpoints: &SQLiteCheckpointStore{db: db},
		Snapshots:   &SQLiteSnapshotStore{db: db},
	}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStateStore) init() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS checkpoints (
			provider TEXT PRIMARY KEY,
			checkpoint TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			provider TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			PRIMARY KEY (provider, entity_type, entity_id)
		)`,
	}
	for _, stmt := range schema {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init sqlite schema: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStateStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteCheckpointStore) Get(provider string) (string, bool) {
	var checkpoint string
	err := s.db.QueryRow(`SELECT checkpoint FROM checkpoints WHERE provider = ?`, provider).Scan(&checkpoint)
	if err != nil {
		return "", false
	}
	return checkpoint, true
}

func (s *SQLiteCheckpointStore) Set(provider, checkpoint string) {
	_, _ = s.db.Exec(`INSERT INTO checkpoints(provider, checkpoint) VALUES(?, ?)
		ON CONFLICT(provider) DO UPDATE SET checkpoint=excluded.checkpoint`, provider, checkpoint)
}

func (s *SQLiteSnapshotStore) Get(provider, entityType, entityID string) (map[string]any, bool) {
	var raw string
	err := s.db.QueryRow(`SELECT snapshot_json FROM snapshots WHERE provider=? AND entity_type=? AND entity_id=?`, provider, entityType, entityID).Scan(&raw)
	if err != nil {
		return nil, false
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, false
	}
	return out, true
}

func (s *SQLiteSnapshotStore) Put(provider, entityType, entityID string, snapshot map[string]any) {
	if snapshot == nil {
		snapshot = map[string]any{}
	}
	b, _ := json.Marshal(snapshot)
	_, _ = s.db.Exec(`INSERT INTO snapshots(provider, entity_type, entity_id, snapshot_json) VALUES(?, ?, ?, ?)
		ON CONFLICT(provider, entity_type, entity_id) DO UPDATE SET snapshot_json=excluded.snapshot_json`,
		provider, entityType, entityID, string(b))
}
