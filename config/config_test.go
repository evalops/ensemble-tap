package config

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
  stream: SIPHON
  subject_prefix: siphon.tap
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
	t.Setenv("VAULT_ADDR", "")

	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load missing config file: %v", err)
	}

	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Fatalf("expected default nats url, got %q", cfg.NATS.URL)
	}
	if cfg.NATS.Stream != "SIPHON" {
		t.Fatalf("expected default stream, got %q", cfg.NATS.Stream)
	}
	if cfg.NATS.ConnectTimeout != 5*time.Second {
		t.Fatalf("expected default nats connect timeout 5s, got %s", cfg.NATS.ConnectTimeout)
	}
	if cfg.NATS.ReconnectWait != 2*time.Second {
		t.Fatalf("expected default nats reconnect wait 2s, got %s", cfg.NATS.ReconnectWait)
	}
	if cfg.NATS.MaxReconnects != -1 {
		t.Fatalf("expected default nats max reconnects -1, got %d", cfg.NATS.MaxReconnects)
	}
	if cfg.NATS.PublishTimeout != 5*time.Second {
		t.Fatalf("expected default nats publish timeout 5s, got %s", cfg.NATS.PublishTimeout)
	}
	if cfg.NATS.PublishMaxRetries != 3 {
		t.Fatalf("expected default nats publish max retries 3, got %d", cfg.NATS.PublishMaxRetries)
	}
	if cfg.NATS.PublishRetryBackoff != 100*time.Millisecond {
		t.Fatalf("expected default nats publish retry backoff 100ms, got %s", cfg.NATS.PublishRetryBackoff)
	}
	if cfg.NATS.StreamReplicas != 1 {
		t.Fatalf("expected default nats stream replicas 1, got %d", cfg.NATS.StreamReplicas)
	}
	if cfg.NATS.StreamStorage != "file" {
		t.Fatalf("expected default nats stream storage file, got %q", cfg.NATS.StreamStorage)
	}
	if cfg.NATS.StreamDiscard != "old" {
		t.Fatalf("expected default nats stream discard old, got %q", cfg.NATS.StreamDiscard)
	}
	if cfg.NATS.StreamMaxConsumers != 0 {
		t.Fatalf("expected default nats stream max consumers 0, got %d", cfg.NATS.StreamMaxConsumers)
	}
	if cfg.NATS.StreamMaxMsgsPerSub != 0 {
		t.Fatalf("expected default nats stream max msgs per subject 0, got %d", cfg.NATS.StreamMaxMsgsPerSub)
	}
	if cfg.NATS.StreamCompression != "none" {
		t.Fatalf("expected default nats stream compression none, got %q", cfg.NATS.StreamCompression)
	}
	if cfg.NATS.StreamAllowMsgTTL {
		t.Fatalf("expected default nats stream allow msg ttl false")
	}
	if cfg.NATS.StreamMaxMsgs != 0 {
		t.Fatalf("expected default nats stream max msgs 0, got %d", cfg.NATS.StreamMaxMsgs)
	}
	if cfg.NATS.StreamMaxBytes != 0 {
		t.Fatalf("expected default nats stream max bytes 0, got %d", cfg.NATS.StreamMaxBytes)
	}
	if cfg.NATS.StreamMaxMsgSize != 0 {
		t.Fatalf("expected default nats stream max msg size 0, got %d", cfg.NATS.StreamMaxMsgSize)
	}
	if cfg.NATS.Secure {
		t.Fatalf("expected default nats secure false")
	}
	if cfg.NATS.InsecureSkipVerify {
		t.Fatalf("expected default nats insecure_skip_verify false")
	}
	if cfg.NATS.CAFile != "" || cfg.NATS.CertFile != "" || cfg.NATS.KeyFile != "" {
		t.Fatalf("expected default nats TLS file paths to be empty")
	}
	if cfg.ClickHouse.Username != "default" {
		t.Fatalf("expected default clickhouse username default, got %q", cfg.ClickHouse.Username)
	}
	if cfg.ClickHouse.TLSServerName != "" || cfg.ClickHouse.CAFile != "" || cfg.ClickHouse.CertFile != "" || cfg.ClickHouse.KeyFile != "" {
		t.Fatalf("expected default clickhouse TLS fields to be empty")
	}
	if cfg.ClickHouse.DialTimeout != 5*time.Second {
		t.Fatalf("expected default clickhouse dial timeout 5s, got %s", cfg.ClickHouse.DialTimeout)
	}
	if cfg.ClickHouse.MaxOpenConns != 4 {
		t.Fatalf("expected default clickhouse max open conns 4, got %d", cfg.ClickHouse.MaxOpenConns)
	}
	if cfg.ClickHouse.MaxIdleConns != 2 {
		t.Fatalf("expected default clickhouse max idle conns 2, got %d", cfg.ClickHouse.MaxIdleConns)
	}
	if cfg.ClickHouse.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("expected default clickhouse conn max lifetime 30m, got %s", cfg.ClickHouse.ConnMaxLifetime)
	}
	if cfg.ClickHouse.ConsumerFetchBatch != 100 {
		t.Fatalf("expected default clickhouse fetch batch size 100, got %d", cfg.ClickHouse.ConsumerFetchBatch)
	}
	if cfg.ClickHouse.ConsumerFetchMaxWait != 500*time.Millisecond {
		t.Fatalf("expected default clickhouse fetch max wait 500ms, got %s", cfg.ClickHouse.ConsumerFetchMaxWait)
	}
	if cfg.ClickHouse.ConsumerAckWait != 30*time.Second {
		t.Fatalf("expected default clickhouse consumer ack wait 30s, got %s", cfg.ClickHouse.ConsumerAckWait)
	}
	if cfg.ClickHouse.ConsumerMaxAckPending != 1000 {
		t.Fatalf("expected default clickhouse consumer max ack pending 1000, got %d", cfg.ClickHouse.ConsumerMaxAckPending)
	}
	if cfg.ClickHouse.ConsumerMaxDeliver != 0 {
		t.Fatalf("expected default clickhouse consumer max deliver 0, got %d", cfg.ClickHouse.ConsumerMaxDeliver)
	}
	if len(cfg.ClickHouse.ConsumerBackoff) != 0 {
		t.Fatalf("expected default clickhouse consumer backoff empty, got %#v", cfg.ClickHouse.ConsumerBackoff)
	}
	if cfg.ClickHouse.ConsumerMaxWaiting != 0 {
		t.Fatalf("expected default clickhouse consumer max waiting 0, got %d", cfg.ClickHouse.ConsumerMaxWaiting)
	}
	if cfg.ClickHouse.ConsumerMaxRequestMaxBytes != 0 {
		t.Fatalf("expected default clickhouse consumer max request max bytes 0, got %d", cfg.ClickHouse.ConsumerMaxRequestMaxBytes)
	}
	if cfg.ClickHouse.InsertTimeout != 10*time.Second {
		t.Fatalf("expected default clickhouse insert timeout 10s, got %s", cfg.ClickHouse.InsertTimeout)
	}
	if cfg.ClickHouse.ConsumerName != "tap_clickhouse_sink" {
		t.Fatalf("expected default clickhouse consumer name tap_clickhouse_sink, got %q", cfg.ClickHouse.ConsumerName)
	}
	if cfg.ClickHouse.RetentionTTL != 365*24*time.Hour {
		t.Fatalf("expected default clickhouse retention ttl 8760h, got %s", cfg.ClickHouse.RetentionTTL)
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
	if cfg.Server.AdminReplayJobTTL != 24*time.Hour {
		t.Fatalf("expected default admin replay job ttl 24h, got %s", cfg.Server.AdminReplayJobTTL)
	}
	if cfg.Server.AdminReplayJobMaxJobs != 512 {
		t.Fatalf("expected default admin replay job max jobs 512, got %d", cfg.Server.AdminReplayJobMaxJobs)
	}
	if cfg.Server.AdminReplayJobTimeout != 5*time.Minute {
		t.Fatalf("expected default admin replay job timeout 5m, got %s", cfg.Server.AdminReplayJobTimeout)
	}
	if cfg.Server.AdminReplayMaxConcurrent != 2 {
		t.Fatalf("expected default admin replay max concurrent jobs 2, got %d", cfg.Server.AdminReplayMaxConcurrent)
	}
	if cfg.Server.AdminReplayStoreBackend != "memory" {
		t.Fatalf("expected default admin replay store backend memory, got %q", cfg.Server.AdminReplayStoreBackend)
	}
	if cfg.Server.AdminReplaySQLitePath != "tap-admin-replay.db" {
		t.Fatalf("expected default admin replay sqlite path, got %q", cfg.Server.AdminReplaySQLitePath)
	}
	if cfg.Server.AdminReplayReasonMinLen != 12 {
		t.Fatalf("expected default admin replay reason min length 12, got %d", cfg.Server.AdminReplayReasonMinLen)
	}
	if cfg.Server.AdminReplayMaxQueuedPerIP != 100 {
		t.Fatalf("expected default admin replay max queued per ip 100, got %d", cfg.Server.AdminReplayMaxQueuedPerIP)
	}
	if cfg.Server.AdminReplayMaxQueuedToken != 20 {
		t.Fatalf("expected default admin replay max queued per token 20, got %d", cfg.Server.AdminReplayMaxQueuedToken)
	}
	if cfg.Server.AdminRateLimitPerSec != 5.0 {
		t.Fatalf("expected default admin rate limit per sec 5.0, got %v", cfg.Server.AdminRateLimitPerSec)
	}
	if cfg.Server.AdminRateLimitBurst != 20 {
		t.Fatalf("expected default admin rate limit burst 20, got %d", cfg.Server.AdminRateLimitBurst)
	}
	if cfg.Server.AdminMTLSClientCertHeader != "X-Forwarded-Client-Cert" {
		t.Fatalf("expected default mTLS client cert header, got %q", cfg.Server.AdminMTLSClientCertHeader)
	}
	if cfg.Vault.Address != "" {
		t.Fatalf("expected default vault address empty, got %q", cfg.Vault.Address)
	}
	if cfg.Vault.AuthMethod != "kubernetes" {
		t.Fatalf("expected default vault auth method kubernetes, got %q", cfg.Vault.AuthMethod)
	}
	if cfg.Vault.KubernetesMountPath != "kubernetes" {
		t.Fatalf("expected default vault kubernetes mount path, got %q", cfg.Vault.KubernetesMountPath)
	}
	if cfg.Vault.KubernetesJWTFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		t.Fatalf("expected default vault kubernetes jwt file, got %q", cfg.Vault.KubernetesJWTFile)
	}
}

func TestLoadConfigResolvesVaultReferencesWithKubernetesAuth(t *testing.T) {
	var loginCalls atomic.Int32
	var readCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/auth/kubernetes/login":
			loginCalls.Add(1)
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode kubernetes login payload: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["role"] != "siphon-runtime" {
				t.Errorf("unexpected vault kubernetes role: %q", payload["role"])
			}
			if payload["jwt"] != "k8s-jwt-token" {
				t.Errorf("unexpected vault kubernetes jwt: %q", payload["jwt"])
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token": "vault-runtime-token",
				},
			})
		case "/v1/secret/data/homelab/siphon/runtime":
			readCalls.Add(1)
			if got := r.Header.Get("X-Vault-Token"); got != "vault-runtime-token" {
				t.Errorf("unexpected vault token header: %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"data": map[string]interface{}{
						"generic-webhook-secret": "vault-webhook-secret",
						"clickhouse-password":    "vault-clickhouse-password",
						"admin-token":            "vault-admin-token",
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	jwtPath := filepath.Join(dir, "jwt")
	if err := os.WriteFile(jwtPath, []byte("k8s-jwt-token"), 0o600); err != nil {
		t.Fatalf("write jwt file: %v", err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	content := `
vault:
  address: ` + server.URL + `
  auth_method: kubernetes
  kubernetes_role: siphon-runtime
  kubernetes_mount_path: kubernetes
  kubernetes_jwt_file: ` + jwtPath + `
providers:
  generic:
    mode: webhook
    secret: 'vault://secret/data/homelab/siphon/runtime#generic-webhook-secret'
clickhouse:
  addr: clickhouse:9000
  password: 'vault://secret/data/homelab/siphon/runtime#clickhouse-password'
server:
  admin_token: 'vault://secret/data/homelab/siphon/runtime#admin-token'
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got := cfg.Providers["generic"].Secret; got != "vault-webhook-secret" {
		t.Fatalf("expected resolved webhook secret, got %q", got)
	}
	if got := cfg.ClickHouse.Password; got != "vault-clickhouse-password" {
		t.Fatalf("expected resolved clickhouse password, got %q", got)
	}
	if got := cfg.Server.AdminToken; got != "vault-admin-token" {
		t.Fatalf("expected resolved admin token, got %q", got)
	}
	if got := loginCalls.Load(); got != 1 {
		t.Fatalf("expected one kubernetes login call, got %d", got)
	}
	if got := readCalls.Load(); got != 1 {
		t.Fatalf("expected one secret read call due to path cache, got %d", got)
	}
}

func TestLoadConfigVaultReferenceRequiresAddress(t *testing.T) {
	t.Setenv("VAULT_ADDR", "")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
vault:
  auth_method: token
  token: test-token
providers:
  generic:
    mode: webhook
    secret: 'vault://secret/data/homelab/siphon/runtime#generic-webhook-secret'
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatalf("expected vault address validation error")
	}
	if !strings.Contains(err.Error(), "vault.address must not be empty") {
		t.Fatalf("expected vault address error, got %v", err)
	}
}

func TestLoadConfigResolvesVaultReferencesWithTokenAuth(t *testing.T) {
	var readCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/secret/data/homelab/siphon/runtime" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		readCalls.Add(1)
		if got := r.Header.Get("X-Vault-Token"); got != "vault-static-token" {
			t.Errorf("unexpected vault token header: %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{
					"value": "vault-default-value",
				},
			},
		})
	}))
	defer server.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
vault:
  address: ` + server.URL + `
  auth_method: token
  token: vault-static-token
providers:
  generic:
    mode: webhook
    secret: 'vault://secret/data/homelab/siphon/runtime'
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got := cfg.Providers["generic"].Secret; got != "vault-default-value" {
		t.Fatalf("expected resolved webhook secret from default key, got %q", got)
	}
	if got := readCalls.Load(); got != 1 {
		t.Fatalf("expected one secret read call, got %d", got)
	}
}

func TestLoadConfigSnakeCaseEnvOverrides(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")

	t.Setenv("TAP_NATS_SUBJECT_PREFIX", "siphon.custom")
	t.Setenv("TAP_NATS_CONNECT_TIMEOUT", "9s")
	t.Setenv("TAP_NATS_MAX_RECONNECTS", "12")
	t.Setenv("TAP_NATS_PUBLISH_MAX_RETRIES", "7")
	t.Setenv("TAP_NATS_USERNAME", "ops-nats")
	t.Setenv("TAP_NATS_PASSWORD", "ops-pass")
	t.Setenv("TAP_NATS_SECURE", "true")
	t.Setenv("TAP_NATS_CA_FILE", "/var/run/secrets/nats/ca.crt")
	t.Setenv("TAP_SERVER_MAX_BODY_SIZE", "2097152")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_LIMIT", "1234")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_TTL", "12h")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_MAX_JOBS", "777")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_TIMEOUT", "2m")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_CONCURRENT_JOBS", "6")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_STORE_BACKEND", "sqlite")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_SQLITE_PATH", "/tmp/tap-admin-replay-test.db")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_REQUIRE_REASON", "true")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_REASON_MIN_LENGTH", "9")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_QUEUED_PER_IP", "55")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_QUEUED_PER_TOKEN", "12")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_PER_SEC", "2.5")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_BURST", "9")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN", "current-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_SECONDARY", "next-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_READ", "read-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_REPLAY", "replay-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_CANCEL", "cancel-admin-token")
	t.Setenv("TAP_CLICKHOUSE_FLUSH_INTERVAL", "3s")
	t.Setenv("TAP_CLICKHOUSE_MAX_OPEN_CONNS", "9")
	t.Setenv("TAP_CLICKHOUSE_USERNAME", "ops")
	t.Setenv("TAP_CLICKHOUSE_TLS_SERVER_NAME", "clickhouse.internal")
	t.Setenv("TAP_CLICKHOUSE_CA_FILE", "/var/run/secrets/clickhouse/ca.crt")
	t.Setenv("TAP_CLICKHOUSE_CONSUMER_NAME", "tap_clickhouse_sink_blue")
	t.Setenv("TAP_CLICKHOUSE_RETENTION_TTL", "720h")
	t.Setenv("TAP_VAULT_ADDRESS", "https://vault.example.internal")
	t.Setenv("TAP_VAULT_NAMESPACE", "homelab")
	t.Setenv("TAP_VAULT_AUTH_METHOD", "token")
	t.Setenv("TAP_VAULT_TOKEN_FILE", "/var/run/secrets/vault/token")
	t.Setenv("TAP_VAULT_KUBERNETES_ROLE", "siphon-runtime")
	t.Setenv("TAP_VAULT_KUBERNETES_MOUNT_PATH", "kubernetes-homelab")
	t.Setenv("TAP_VAULT_KUBERNETES_JWT_FILE", "/var/run/secrets/kubernetes.io/serviceaccount/token")
	t.Setenv("TAP_PROVIDERS_STRIPE_SECRET", "whsec_env")
	t.Setenv("TAP_PROVIDERS_HUBSPOT_CLIENT_SECRET", "hs_client_secret")

	cfg, err := Load(missing)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.NATS.SubjectPrefix != "siphon.custom" {
		t.Fatalf("expected nats.subject_prefix override, got %q", cfg.NATS.SubjectPrefix)
	}
	if cfg.NATS.ConnectTimeout != 9*time.Second {
		t.Fatalf("expected nats.connect_timeout override, got %s", cfg.NATS.ConnectTimeout)
	}
	if cfg.NATS.MaxReconnects != 12 {
		t.Fatalf("expected nats.max_reconnects override, got %d", cfg.NATS.MaxReconnects)
	}
	if cfg.NATS.PublishMaxRetries != 7 {
		t.Fatalf("expected nats.publish_max_retries override, got %d", cfg.NATS.PublishMaxRetries)
	}
	if cfg.NATS.Username != "ops-nats" || cfg.NATS.Password != "ops-pass" {
		t.Fatalf("expected nats username/password override, got %q/%q", cfg.NATS.Username, cfg.NATS.Password)
	}
	if !cfg.NATS.Secure {
		t.Fatalf("expected nats.secure override true")
	}
	if cfg.NATS.CAFile != "/var/run/secrets/nats/ca.crt" {
		t.Fatalf("expected nats.ca_file override, got %q", cfg.NATS.CAFile)
	}
	if cfg.Server.MaxBodySize != 2097152 {
		t.Fatalf("expected server.max_body_size override, got %d", cfg.Server.MaxBodySize)
	}
	if cfg.Server.AdminReplayMaxLimit != 1234 {
		t.Fatalf("expected server.admin_replay_max_limit override, got %d", cfg.Server.AdminReplayMaxLimit)
	}
	if cfg.Server.AdminReplayJobTTL != 12*time.Hour {
		t.Fatalf("expected server.admin_replay_job_ttl override, got %s", cfg.Server.AdminReplayJobTTL)
	}
	if cfg.Server.AdminReplayJobMaxJobs != 777 {
		t.Fatalf("expected server.admin_replay_job_max_jobs override, got %d", cfg.Server.AdminReplayJobMaxJobs)
	}
	if cfg.Server.AdminReplayJobTimeout != 2*time.Minute {
		t.Fatalf("expected server.admin_replay_job_timeout override, got %s", cfg.Server.AdminReplayJobTimeout)
	}
	if cfg.Server.AdminReplayMaxConcurrent != 6 {
		t.Fatalf("expected server.admin_replay_max_concurrent_jobs override, got %d", cfg.Server.AdminReplayMaxConcurrent)
	}
	if cfg.Server.AdminReplayStoreBackend != "sqlite" {
		t.Fatalf("expected server.admin_replay_store_backend override, got %q", cfg.Server.AdminReplayStoreBackend)
	}
	if cfg.Server.AdminReplaySQLitePath != "/tmp/tap-admin-replay-test.db" {
		t.Fatalf("expected server.admin_replay_sqlite_path override, got %q", cfg.Server.AdminReplaySQLitePath)
	}
	if !cfg.Server.AdminReplayRequireReason {
		t.Fatalf("expected server.admin_replay_require_reason override")
	}
	if cfg.Server.AdminReplayReasonMinLen != 9 {
		t.Fatalf("expected server.admin_replay_reason_min_length override, got %d", cfg.Server.AdminReplayReasonMinLen)
	}
	if cfg.Server.AdminReplayMaxQueuedPerIP != 55 {
		t.Fatalf("expected server.admin_replay_max_queued_per_ip override, got %d", cfg.Server.AdminReplayMaxQueuedPerIP)
	}
	if cfg.Server.AdminReplayMaxQueuedToken != 12 {
		t.Fatalf("expected server.admin_replay_max_queued_per_token override, got %d", cfg.Server.AdminReplayMaxQueuedToken)
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
	if cfg.Server.AdminTokenRead != "read-admin-token" {
		t.Fatalf("expected server.admin_token_read override")
	}
	if cfg.Server.AdminTokenReplay != "replay-admin-token" {
		t.Fatalf("expected server.admin_token_replay override")
	}
	if cfg.Server.AdminTokenCancel != "cancel-admin-token" {
		t.Fatalf("expected server.admin_token_cancel override")
	}
	if cfg.ClickHouse.FlushInterval != 3*time.Second {
		t.Fatalf("expected clickhouse.flush_interval override, got %s", cfg.ClickHouse.FlushInterval)
	}
	if cfg.ClickHouse.MaxOpenConns != 9 {
		t.Fatalf("expected clickhouse.max_open_conns override, got %d", cfg.ClickHouse.MaxOpenConns)
	}
	if cfg.ClickHouse.Username != "ops" {
		t.Fatalf("expected clickhouse.username override, got %q", cfg.ClickHouse.Username)
	}
	if cfg.ClickHouse.TLSServerName != "clickhouse.internal" {
		t.Fatalf("expected clickhouse.tls_server_name override, got %q", cfg.ClickHouse.TLSServerName)
	}
	if cfg.ClickHouse.CAFile != "/var/run/secrets/clickhouse/ca.crt" {
		t.Fatalf("expected clickhouse.ca_file override, got %q", cfg.ClickHouse.CAFile)
	}
	if cfg.ClickHouse.ConsumerName != "tap_clickhouse_sink_blue" {
		t.Fatalf("expected clickhouse.consumer_name override, got %q", cfg.ClickHouse.ConsumerName)
	}
	if cfg.ClickHouse.RetentionTTL != 720*time.Hour {
		t.Fatalf("expected clickhouse.retention_ttl override, got %s", cfg.ClickHouse.RetentionTTL)
	}
	if cfg.Vault.Address != "https://vault.example.internal" {
		t.Fatalf("expected vault.address override, got %q", cfg.Vault.Address)
	}
	if cfg.Vault.Namespace != "homelab" {
		t.Fatalf("expected vault.namespace override, got %q", cfg.Vault.Namespace)
	}
	if cfg.Vault.AuthMethod != "token" {
		t.Fatalf("expected vault.auth_method override, got %q", cfg.Vault.AuthMethod)
	}
	if cfg.Vault.TokenFile != "/var/run/secrets/vault/token" {
		t.Fatalf("expected vault.token_file override, got %q", cfg.Vault.TokenFile)
	}
	if cfg.Vault.KubernetesRole != "siphon-runtime" {
		t.Fatalf("expected vault.kubernetes_role override, got %q", cfg.Vault.KubernetesRole)
	}
	if cfg.Vault.KubernetesMountPath != "kubernetes-homelab" {
		t.Fatalf("expected vault.kubernetes_mount_path override, got %q", cfg.Vault.KubernetesMountPath)
	}
	if cfg.Vault.KubernetesJWTFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		t.Fatalf("expected vault.kubernetes_jwt_file override, got %q", cfg.Vault.KubernetesJWTFile)
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
			name: "replay job ttl must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobTTL: -1 * time.Second,
				},
			},
			wantErrSub: "admin_replay_job_ttl",
		},
		{
			name: "replay job max jobs upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobMaxJobs: 100001,
				},
			},
			wantErrSub: "admin_replay_job_max_jobs",
		},
		{
			name: "replay job timeout must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobTimeout: -1 * time.Second,
				},
			},
			wantErrSub: "admin_replay_job_timeout",
		},
		{
			name: "replay max concurrent jobs upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxConcurrent: 101,
				},
			},
			wantErrSub: "admin_replay_max_concurrent_jobs",
		},
		{
			name: "replay store backend must be valid",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayStoreBackend: "file",
				},
			},
			wantErrSub: "admin_replay_store_backend",
		},
		{
			name: "sqlite replay backend accepts default sqlite path",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayStoreBackend: "sqlite",
				},
			},
		},
		{
			name: "replay reason min length must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayReasonMinLen: -1,
				},
			},
			wantErrSub: "admin_replay_reason_min_length",
		},
		{
			name: "replay queued per ip must be in range",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxQueuedPerIP: -1,
				},
			},
			wantErrSub: "admin_replay_max_queued_per_ip",
		},
		{
			name: "replay queued per token must be in range",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxQueuedToken: -1,
				},
			},
			wantErrSub: "admin_replay_max_queued_per_token",
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
			name: "admin allowlist CIDR must be valid",
			cfg: Config{
				Server: ServerConfig{
					AdminAllowedCIDRs: []string{"not-a-cidr"},
				},
			},
			wantErrSub: "admin_allowed_cidrs",
		},
		{
			name: "valid token rotation and replay max",
			cfg: Config{
				Server: ServerConfig{
					AdminToken:                "primary-token",
					AdminTokenSecondary:       "next-token",
					AdminReplayMaxLimit:       5000,
					AdminReplayJobTTL:         12 * time.Hour,
					AdminReplayJobMaxJobs:     2048,
					AdminReplayJobTimeout:     2 * time.Minute,
					AdminReplayMaxConcurrent:  4,
					AdminReplayStoreBackend:   "sqlite",
					AdminReplaySQLitePath:     "/tmp/tap-admin-replay.db",
					AdminReplayRequireReason:  true,
					AdminReplayReasonMinLen:   16,
					AdminReplayMaxQueuedPerIP: 80,
					AdminReplayMaxQueuedToken: 10,
					AdminRateLimitPerSec:      3.5,
					AdminRateLimitBurst:       11,
					AdminAllowedCIDRs:         []string{"203.0.113.0/24"},
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

func TestConfigValidateNATSAndClickHouseRules(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErrSub string
	}{
		{
			name: "nats subject prefix rejects wildcard",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.>",
				},
			},
			wantErrSub: "nats.subject_prefix",
		},
		{
			name: "nats dedup window must not exceed max age",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
					MaxAge:        time.Minute,
					DedupWindow:   2 * time.Minute,
				},
			},
			wantErrSub: "nats.dedup_window",
		},
		{
			name: "nats password requires username",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
					Password:      "secret",
				},
			},
			wantErrSub: "nats.password requires nats.username",
		},
		{
			name: "nats creds file cannot mix with token",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
					CredsFile:     "/tmp/nats.creds",
					Token:         "nats-token",
				},
			},
			wantErrSub: "nats.creds_file",
		},
		{
			name: "nats url rejects unsupported scheme",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "http://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
			},
			wantErrSub: "unsupported scheme",
		},
		{
			name: "nats url rejects empty endpoint entries",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222,",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
			},
			wantErrSub: "contains empty endpoint",
		},
		{
			name: "nats url rejects invalid endpoint ports",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:70000",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
			},
			wantErrSub: "invalid port",
		},
		{
			name: "nats insecure skip verify requires secure",
			cfg: Config{
				NATS: NATSConfig{
					URL:                "nats://localhost:4222",
					Stream:             "SIPHON",
					SubjectPrefix:      "siphon.tap",
					InsecureSkipVerify: true,
				},
			},
			wantErrSub: "nats.insecure_skip_verify",
		},
		{
			name: "nats cert and key files must be paired",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
					Secure:        true,
					CertFile:      "/var/run/secrets/nats/client.crt",
				},
			},
			wantErrSub: "nats.cert_file and nats.key_file",
		},
		{
			name: "nats stream max msg size upper bound",
			cfg: Config{
				NATS: NATSConfig{
					URL:              "nats://localhost:4222",
					Stream:           "SIPHON",
					SubjectPrefix:    "siphon.tap",
					StreamMaxMsgSize: 2147483648,
				},
			},
			wantErrSub: "nats.stream_max_msg_size",
		},
		{
			name: "clickhouse insecure skip verify requires secure",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:               "clickhouse:9000",
					InsecureSkipVerify: true,
				},
			},
			wantErrSub: "clickhouse.insecure_skip_verify",
		},
		{
			name: "clickhouse tls server name requires secure",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:          "clickhouse:9000",
					TLSServerName: "clickhouse.internal",
				},
			},
			wantErrSub: "clickhouse.tls_server_name",
		},
		{
			name: "clickhouse cert and key files must be paired",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:                  "clickhouse:9000",
					Secure:                true,
					CertFile:              "/var/run/secrets/clickhouse/client.crt",
					ConsumerName:          "tap_clickhouse_sink",
					Database:              "siphon",
					Table:                 "tap_events",
					Username:              "default",
					DialTimeout:           5 * time.Second,
					MaxOpenConns:          4,
					MaxIdleConns:          2,
					BatchSize:             500,
					FlushInterval:         2 * time.Second,
					ConsumerFetchBatch:    100,
					ConsumerFetchMaxWait:  500 * time.Millisecond,
					ConsumerAckWait:       30 * time.Second,
					ConsumerMaxAckPending: 1000,
					InsertTimeout:         10 * time.Second,
					RetentionTTL:          365 * 24 * time.Hour,
				},
			},
			wantErrSub: "clickhouse.cert_file and clickhouse.key_file",
		},
		{
			name: "clickhouse addr requires host and port",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr: "clickhouse",
				},
			},
			wantErrSub: "clickhouse.addr",
		},
		{
			name: "clickhouse addr rejects out-of-range port",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr: "clickhouse:70000",
				},
			},
			wantErrSub: "invalid port",
		},
		{
			name: "clickhouse insert timeout must be less than ack wait",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:                  "clickhouse:9000",
					ConsumerAckWait:       30 * time.Second,
					InsertTimeout:         30 * time.Second,
					ConsumerName:          "tap_clickhouse_sink",
					Database:              "siphon",
					Table:                 "tap_events",
					Username:              "default",
					DialTimeout:           5 * time.Second,
					MaxOpenConns:          4,
					MaxIdleConns:          2,
					BatchSize:             500,
					FlushInterval:         2 * time.Second,
					ConsumerFetchBatch:    100,
					ConsumerFetchMaxWait:  500 * time.Millisecond,
					ConsumerMaxAckPending: 1000,
					RetentionTTL:          365 * 24 * time.Hour,
				},
			},
			wantErrSub: "clickhouse.insert_timeout",
		},
		{
			name: "clickhouse fetch max wait must be less than ack wait",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:                 "clickhouse:9000",
					ConsumerFetchMaxWait: 30 * time.Second,
					ConsumerAckWait:      30 * time.Second,
				},
			},
			wantErrSub: "clickhouse.consumer_fetch_max_wait",
		},
		{
			name: "clickhouse flush interval must be less than ack wait",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:            "clickhouse:9000",
					FlushInterval:   30 * time.Second,
					ConsumerAckWait: 30 * time.Second,
				},
			},
			wantErrSub: "clickhouse.flush_interval",
		},
		{
			name: "clickhouse insert and flush combined must fit under ack wait",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:            "clickhouse:9000",
					InsertTimeout:   20 * time.Second,
					FlushInterval:   15 * time.Second,
					ConsumerAckWait: 30 * time.Second,
				},
			},
			wantErrSub: "insert_timeout + clickhouse.flush_interval",
		},
		{
			name: "clickhouse consumer name cannot contain whitespace",
			cfg: Config{
				NATS: NATSConfig{
					URL:           "nats://localhost:4222",
					Stream:        "SIPHON",
					SubjectPrefix: "siphon.tap",
				},
				ClickHouse: ClickHouseConfig{
					Addr:                  "clickhouse:9000",
					ConsumerName:          "tap clickhouse sink",
					Database:              "siphon",
					Table:                 "tap_events",
					Username:              "default",
					DialTimeout:           5 * time.Second,
					MaxOpenConns:          4,
					MaxIdleConns:          2,
					BatchSize:             500,
					FlushInterval:         2 * time.Second,
					ConsumerFetchBatch:    100,
					ConsumerFetchMaxWait:  500 * time.Millisecond,
					ConsumerAckWait:       30 * time.Second,
					ConsumerMaxAckPending: 1000,
					InsertTimeout:         10 * time.Second,
					RetentionTTL:          365 * 24 * time.Hour,
				},
			},
			wantErrSub: "clickhouse.consumer_name",
		},
		{
			name: "valid nats and clickhouse tuning config",
			cfg: Config{
				NATS: NATSConfig{
					URL:                 "nats://localhost:4222",
					Stream:              "SIPHON",
					SubjectPrefix:       "siphon.tap",
					MaxAge:              24 * time.Hour,
					DedupWindow:         time.Minute,
					ConnectTimeout:      5 * time.Second,
					ReconnectWait:       2 * time.Second,
					MaxReconnects:       -1,
					PublishTimeout:      5 * time.Second,
					PublishMaxRetries:   3,
					PublishRetryBackoff: 100 * time.Millisecond,
					Username:            "nats-user",
					Password:            "nats-pass",
					Secure:              true,
					CAFile:              "/var/run/secrets/nats/ca.crt",
					StreamReplicas:      1,
					StreamStorage:       "file",
					StreamDiscard:       "old",
					StreamMaxMsgs:       1000000,
					StreamMaxBytes:      104857600,
					StreamMaxMsgSize:    1048576,
				},
				ClickHouse: ClickHouseConfig{
					Addr:                  "clickhouse-a:9000,clickhouse-b:9000",
					Database:              "siphon",
					Table:                 "tap_events",
					Username:              "default",
					Password:              "secret",
					Secure:                true,
					TLSServerName:         "clickhouse.internal",
					CAFile:                "/var/run/secrets/clickhouse/ca.crt",
					DialTimeout:           5 * time.Second,
					MaxOpenConns:          8,
					MaxIdleConns:          4,
					ConnMaxLifetime:       30 * time.Minute,
					BatchSize:             500,
					FlushInterval:         2 * time.Second,
					ConsumerName:          "tap_clickhouse_sink_prod",
					ConsumerFetchBatch:    100,
					ConsumerFetchMaxWait:  500 * time.Millisecond,
					ConsumerAckWait:       30 * time.Second,
					ConsumerMaxAckPending: 1000,
					InsertTimeout:         10 * time.Second,
					RetentionTTL:          365 * 24 * time.Hour,
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

func TestConfigValidateProviderModesAndCredentials(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErrSub string
	}{
		{
			name: "webhook provider requires secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"stripe": {Mode: "webhook"},
				},
			},
			wantErrSub: "providers.stripe webhook mode requires secret",
		},
		{
			name: "hubspot webhook accepts client secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {Mode: "webhook", ClientSecret: "hs-client-secret"},
				},
			},
		},
		{
			name: "webhook provider accepts tenant secret override",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"acme": {
						Mode: "webhook",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {Secret: "tenant-secret"},
						},
					},
				},
			},
		},
		{
			name: "unsupported poll provider fails fast",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"stripe": {Mode: "poll", AccessToken: "tok", BaseURL: "https://api.example.com"},
				},
			},
			wantErrSub: "providers.stripe poll mode is not supported",
		},
		{
			name: "salesforce poll requires base url",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"salesforce": {Mode: "poll", AccessToken: "tok"},
				},
			},
			wantErrSub: "providers.salesforce poll target base is invalid: base_url is required",
		},
		{
			name: "quickbooks poll requires realm id",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"quickbooks": {Mode: "poll", AccessToken: "tok", BaseURL: "https://quickbooks.api.intuit.com"},
				},
			},
			wantErrSub: "realm_id is required",
		},
		{
			name: "hubspot poll supports tenant-only credentials",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {
						Mode: "poll",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {
								AccessToken: "tenant-token",
								BaseURL:     "https://api.hubapi.com",
							},
						},
					},
				},
			},
		},
		{
			name: "hubspot poll validates base target when base credentials present",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {
						Mode:        "poll",
						AccessToken: "base-token",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {
								AccessToken: "tenant-token",
								BaseURL:     "https://api.hubapi.com",
							},
						},
					},
				},
			},
			wantErrSub: "providers.hubspot poll target base is invalid: base_url is required",
		},
		{
			name: "notion poll accepts base secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"notion": {Mode: "poll", Secret: "notion-token", BaseURL: "https://api.notion.com"},
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

func TestConfigValidateAdvancedNATSAndClickHouseControls(t *testing.T) {
	baseConfig := func() Config {
		return Config{
			NATS: NATSConfig{
				URL:                 "nats://localhost:4222",
				Stream:              "SIPHON",
				SubjectPrefix:       "siphon.tap",
				MaxAge:              24 * time.Hour,
				DedupWindow:         time.Minute,
				ConnectTimeout:      5 * time.Second,
				ReconnectWait:       2 * time.Second,
				MaxReconnects:       -1,
				PublishTimeout:      5 * time.Second,
				PublishMaxRetries:   3,
				PublishRetryBackoff: 100 * time.Millisecond,
				StreamReplicas:      1,
				StreamStorage:       "file",
				StreamDiscard:       "old",
				StreamCompression:   "none",
			},
			ClickHouse: ClickHouseConfig{
				Addr:                  "clickhouse:9000",
				Database:              "siphon",
				Table:                 "tap_events",
				Username:              "default",
				DialTimeout:           5 * time.Second,
				MaxOpenConns:          4,
				MaxIdleConns:          2,
				BatchSize:             500,
				FlushInterval:         2 * time.Second,
				ConsumerName:          "tap_clickhouse_sink",
				ConsumerFetchBatch:    100,
				ConsumerFetchMaxWait:  500 * time.Millisecond,
				ConsumerAckWait:       30 * time.Second,
				ConsumerMaxAckPending: 1000,
				InsertTimeout:         10 * time.Second,
				RetentionTTL:          365 * 24 * time.Hour,
			},
		}
	}

	tests := []struct {
		name       string
		mutate     func(*Config)
		wantErrSub string
	}{
		{
			name: "nats stream max consumers must be non-negative",
			mutate: func(cfg *Config) {
				cfg.NATS.StreamMaxConsumers = -1
			},
			wantErrSub: "nats.stream_max_consumers",
		},
		{
			name: "nats stream max msgs per subject must be non-negative",
			mutate: func(cfg *Config) {
				cfg.NATS.StreamMaxMsgsPerSub = -1
			},
			wantErrSub: "nats.stream_max_msgs_per_subject",
		},
		{
			name: "nats stream compression must be supported",
			mutate: func(cfg *Config) {
				cfg.NATS.StreamCompression = "gzip"
			},
			wantErrSub: "nats.stream_compression",
		},
		{
			name: "clickhouse consumer max deliver must be >= -1",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerMaxDeliver = -2
			},
			wantErrSub: "clickhouse.consumer_max_deliver",
		},
		{
			name: "clickhouse consumer max waiting must be non-negative",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerMaxWaiting = -1
			},
			wantErrSub: "clickhouse.consumer_max_waiting",
		},
		{
			name: "clickhouse consumer max request max bytes must be non-negative",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerMaxRequestMaxBytes = -1
			},
			wantErrSub: "clickhouse.consumer_max_request_max_bytes",
		},
		{
			name: "clickhouse consumer backoff must be positive",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerBackoff = []time.Duration{100 * time.Millisecond, 0}
			},
			wantErrSub: "clickhouse.consumer_backoff[1]",
		},
		{
			name: "clickhouse consumer backoff must be non-decreasing",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerBackoff = []time.Duration{200 * time.Millisecond, 100 * time.Millisecond}
			},
			wantErrSub: "clickhouse.consumer_backoff must be non-decreasing",
		},
		{
			name: "clickhouse max deliver must match backoff length",
			mutate: func(cfg *Config) {
				cfg.ClickHouse.ConsumerMaxDeliver = 3
				cfg.ClickHouse.ConsumerBackoff = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
			},
			wantErrSub: "clickhouse.consumer_max_deliver must equal len(clickhouse.consumer_backoff)",
		},
		{
			name: "valid advanced stream and consumer controls",
			mutate: func(cfg *Config) {
				cfg.NATS.StreamMaxConsumers = 64
				cfg.NATS.StreamMaxMsgsPerSub = 1000
				cfg.NATS.StreamCompression = "s2"
				cfg.NATS.StreamAllowMsgTTL = true
				cfg.ClickHouse.ConsumerMaxDeliver = 3
				cfg.ClickHouse.ConsumerBackoff = []time.Duration{
					100 * time.Millisecond,
					200 * time.Millisecond,
					400 * time.Millisecond,
				}
				cfg.ClickHouse.ConsumerMaxWaiting = 512
				cfg.ClickHouse.ConsumerMaxRequestMaxBytes = 1048576
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
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
