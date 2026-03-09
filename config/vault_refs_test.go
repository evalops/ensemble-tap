package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadFileWithinRoot(t *testing.T) {
	dir := t.TempDir()
	absPath := filepath.Join(dir, "vault-token")
	if err := os.WriteFile(absPath, []byte("token-123"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	got, err := readFileWithinRoot(absPath)
	if err != nil {
		t.Fatalf("readFileWithinRoot absolute path: %v", err)
	}
	if string(got) != "token-123" {
		t.Fatalf("expected token-123, got %q", string(got))
	}

	t.Chdir(dir)
	got, err = readFileWithinRoot("vault-token")
	if err != nil {
		t.Fatalf("readFileWithinRoot relative path: %v", err)
	}
	if string(got) != "token-123" {
		t.Fatalf("expected token-123 from relative read, got %q", string(got))
	}
}

func TestReadFileWithinRootMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := readFileWithinRoot(filepath.Join(dir, "does-not-exist"))
	if err == nil {
		t.Fatalf("expected missing file error")
	}
}
