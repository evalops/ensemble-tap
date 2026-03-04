package config

import (
	"os"
	"path/filepath"
	"strings"
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
	if cfg.Server.AdminReplayMaxLimit != 2000 {
		t.Fatalf("expected default admin replay max limit 2000, got %d", cfg.Server.AdminReplayMaxLimit)
	}
	if cfg.Server.AdminRateLimitPerSec != 5.0 {
		t.Fatalf("expected default admin rate limit per sec 5.0, got %v", cfg.Server.AdminRateLimitPerSec)
	}
	if cfg.Server.AdminRateLimitBurst != 20 {
		t.Fatalf("expected default admin rate limit burst 20, got %d", cfg.Server.AdminRateLimitBurst)
	}
}

func TestLoadConfigSnakeCaseEnvOverrides(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")

	t.Setenv("TAP_NATS_SUBJECT_PREFIX", "ensemble.tap.custom")
	t.Setenv("TAP_SERVER_MAX_BODY_SIZE", "2097152")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_LIMIT", "1234")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_PER_SEC", "2.5")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_BURST", "9")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN", "current-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_SECONDARY", "next-admin-token")
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
	if cfg.Server.AdminReplayMaxLimit != 1234 {
		t.Fatalf("expected server.admin_replay_max_limit override, got %d", cfg.Server.AdminReplayMaxLimit)
	}
	if cfg.Server.AdminRateLimitPerSec != 2.5 {
		t.Fatalf("expected server.admin_rate_limit_per_sec override, got %v", cfg.Server.AdminRateLimitPerSec)
	}
	if cfg.Server.AdminRateLimitBurst != 9 {
		t.Fatalf("expected server.admin_rate_limit_burst override, got %d", cfg.Server.AdminRateLimitBurst)
	}
	if cfg.Server.AdminToken != "current-admin-token" {
		t.Fatalf("expected server.admin_token override")
	}
	if cfg.Server.AdminTokenSecondary != "next-admin-token" {
		t.Fatalf("expected server.admin_token_secondary override")
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

func TestConfigValidateAdminTokenAndReplayRules(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErrSub string
	}{
		{
			name: "secondary token requires primary",
			cfg: Config{
				Server: ServerConfig{
					AdminTokenSecondary: "next-token",
				},
			},
			wantErrSub: "admin_token_secondary requires",
		},
		{
			name: "primary and secondary token must differ",
			cfg: Config{
				Server: ServerConfig{
					AdminToken:          "same-token",
					AdminTokenSecondary: "same-token",
				},
			},
			wantErrSub: "must differ",
		},
		{
			name: "replay max limit must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxLimit: -1,
				},
			},
			wantErrSub: "must be in range",
		},
		{
			name: "replay max limit upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxLimit: 100001,
				},
			},
			wantErrSub: "must be in range",
		},
		{
			name: "admin rate limit per sec must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminRateLimitPerSec: -1,
				},
			},
			wantErrSub: "admin_rate_limit_per_sec",
		},
		{
			name: "admin rate limit burst must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminRateLimitBurst: -1,
				},
			},
			wantErrSub: "admin_rate_limit_burst",
		},
		{
			name: "valid token rotation and replay max",
			cfg: Config{
				Server: ServerConfig{
					AdminToken:           "primary-token",
					AdminTokenSecondary:  "next-token",
					AdminReplayMaxLimit:  5000,
					AdminRateLimitPerSec: 3.5,
					AdminRateLimitBurst:  11,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.ApplyDefaults()
			err := cfg.Validate()
			if tt.wantErrSub == "" {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation error containing %q", tt.wantErrSub)
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("expected validation error containing %q, got %q", tt.wantErrSub, err.Error())
			}
		})
	}
}
