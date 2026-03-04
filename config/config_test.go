package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigWithEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
providers:
  stripe:
    mode: webhook
    secret: ${STRIPE_WEBHOOK_SECRET}
nats:
  url: nats://localhost:4222
  stream: ENSEMBLE_TAP
  subject_prefix: ensemble.tap
  max_age: 168h
  dedup_window: 2m
server:
  port: 8080
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_123")
	t.Setenv("TAP_SERVER_PORT", "9091")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got := cfg.Server.Port; got != 9091 {
		t.Fatalf("expected env override port 9091, got %d", got)
	}
	if cfg.Providers["stripe"].Secret != "whsec_123" {
		t.Fatalf("expected secret expansion")
	}
	if cfg.NATS.MaxAge != 168*time.Hour {
		t.Fatalf("expected max_age 168h, got %s", cfg.NATS.MaxAge)
	}
}

func TestLoadConfigMissingFileAppliesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load missing config file: %v", err)
	}

	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Fatalf("expected default nats url, got %q", cfg.NATS.URL)
	}
	if cfg.NATS.Stream != "ENSEMBLE_TAP" {
		t.Fatalf("expected default stream, got %q", cfg.NATS.Stream)
	}
	if cfg.Server.Port != 8080 {
		t.Fatalf("expected default server port, got %d", cfg.Server.Port)
	}
	if cfg.Server.BasePath != "/webhooks" {
		t.Fatalf("expected default base path, got %q", cfg.Server.BasePath)
	}
}

func TestLoadConfigSnakeCaseEnvOverrides(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")

	t.Setenv("TAP_NATS_SUBJECT_PREFIX", "ensemble.tap.custom")
	t.Setenv("TAP_SERVER_MAX_BODY_SIZE", "2097152")
	t.Setenv("TAP_CLICKHOUSE_FLUSH_INTERVAL", "3s")
	t.Setenv("TAP_PROVIDERS_STRIPE_SECRET", "whsec_env")
	t.Setenv("TAP_PROVIDERS_HUBSPOT_CLIENT_SECRET", "hs_client_secret")

	cfg, err := Load(missing)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.NATS.SubjectPrefix != "ensemble.tap.custom" {
		t.Fatalf("expected nats.subject_prefix override, got %q", cfg.NATS.SubjectPrefix)
	}
	if cfg.Server.MaxBodySize != 2097152 {
		t.Fatalf("expected server.max_body_size override, got %d", cfg.Server.MaxBodySize)
	}
	if cfg.ClickHouse.FlushInterval != 3*time.Second {
		t.Fatalf("expected clickhouse.flush_interval override, got %s", cfg.ClickHouse.FlushInterval)
	}
	if cfg.Providers["stripe"].Secret != "whsec_env" {
		t.Fatalf("expected providers.stripe.secret override")
	}
	if cfg.Providers["hubspot"].ClientSecret != "hs_client_secret" {
		t.Fatalf("expected providers.hubspot.client_secret override")
	}
}
