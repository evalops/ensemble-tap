package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	defaultConfigPath          = "config.yaml"
	envPrefix                  = "TAP_"
	defaultAdminReplayMaxLimit = 2000
	maxAdminReplayMaxLimit     = 100000
)

type Config struct {
	Providers  map[string]ProviderConfig `koanf:"providers"`
	NATS       NATSConfig                `koanf:"nats"`
	ClickHouse ClickHouseConfig          `koanf:"clickhouse"`
	Server     ServerConfig              `koanf:"server"`
	State      StateConfig               `koanf:"state"`
}

type ProviderConfig struct {
	Mode                 string                          `koanf:"mode"`
	Secret               string                          `koanf:"secret"`
	Events               []string                        `koanf:"events"`
	PollInterval         time.Duration                   `koanf:"poll_interval"`
	PollRateLimitPerSec  float64                         `koanf:"poll_rate_limit_per_sec"`
	PollBurst            int                             `koanf:"poll_burst"`
	PollFailureBudget    int                             `koanf:"poll_failure_budget"`
	PollCircuitBreak     time.Duration                   `koanf:"poll_circuit_break_duration"`
	PollJitterRatio      float64                         `koanf:"poll_jitter_ratio"`
	BaseURL              string                          `koanf:"base_url"`
	AccessToken          string                          `koanf:"access_token"`
	Objects              []string                        `koanf:"objects"`
	RealmID              string                          `koanf:"realm_id"`
	APIVersion           string                          `koanf:"api_version"`
	QueryPerPage         int                             `koanf:"query_per_page"`
	TokenURL             string                          `koanf:"token_url"`
	ClientID             string                          `koanf:"client_id"`
	Scope                string                          `koanf:"scope"`
	RefreshToken         string                          `koanf:"refresh_token"`
	TenantID             string                          `koanf:"tenant_id"`
	AppID                string                          `koanf:"app_id"`
	ClientSecret         string                          `koanf:"client_secret"`
	APIKey               string                          `koanf:"api_key"`
	WebhookVerifierToken string                          `koanf:"webhook_verifier_token"`
	Tenants              map[string]ProviderTenantConfig `koanf:"tenants"`
}

type ProviderTenantConfig struct {
	TenantID            string        `koanf:"tenant_id"`
	Secret              string        `koanf:"secret"`
	ClientSecret        string        `koanf:"client_secret"`
	AccessToken         string        `koanf:"access_token"`
	APIKey              string        `koanf:"api_key"`
	BaseURL             string        `koanf:"base_url"`
	RealmID             string        `koanf:"realm_id"`
	RefreshToken        string        `koanf:"refresh_token"`
	PollInterval        time.Duration `koanf:"poll_interval"`
	PollRateLimitPerSec float64       `koanf:"poll_rate_limit_per_sec"`
	PollBurst           int           `koanf:"poll_burst"`
	PollFailureBudget   int           `koanf:"poll_failure_budget"`
	PollCircuitBreak    time.Duration `koanf:"poll_circuit_break_duration"`
	PollJitterRatio     float64       `koanf:"poll_jitter_ratio"`
}

type NATSConfig struct {
	URL                  string        `koanf:"url"`
	Stream               string        `koanf:"stream"`
	SubjectPrefix        string        `koanf:"subject_prefix"`
	TenantScopedSubjects bool          `koanf:"tenant_scoped_subjects"`
	MaxAge               time.Duration `koanf:"max_age"`
	DedupWindow          time.Duration `koanf:"dedup_window"`
}

type ClickHouseConfig struct {
	Addr          string        `koanf:"addr"`
	Database      string        `koanf:"database"`
	Table         string        `koanf:"table"`
	BatchSize     int           `koanf:"batch_size"`
	FlushInterval time.Duration `koanf:"flush_interval"`
}

type ServerConfig struct {
	Port                int           `koanf:"port"`
	BasePath            string        `koanf:"base_path"`
	ReadTimeout         time.Duration `koanf:"read_timeout"`
	WriteTimeout        time.Duration `koanf:"write_timeout"`
	MaxBodySize         int64         `koanf:"max_body_size"`
	AdminToken          string        `koanf:"admin_token"`
	AdminTokenSecondary string        `koanf:"admin_token_secondary"`
	AdminReplayMaxLimit int           `koanf:"admin_replay_max_limit"`
}

type StateConfig struct {
	Backend    string `koanf:"backend"`
	SQLitePath string `koanf:"sqlite_path"`
}

func (c *Config) ApplyDefaults() {
	if c.Providers == nil {
		c.Providers = make(map[string]ProviderConfig)
	}
	if c.NATS.URL == "" {
		c.NATS.URL = "nats://localhost:4222"
	}
	if c.NATS.Stream == "" {
		c.NATS.Stream = "ENSEMBLE_TAP"
	}
	if c.NATS.SubjectPrefix == "" {
		c.NATS.SubjectPrefix = "ensemble.tap"
	}
	if c.NATS.MaxAge == 0 {
		c.NATS.MaxAge = 7 * 24 * time.Hour
	}
	if c.NATS.DedupWindow == 0 {
		c.NATS.DedupWindow = 2 * time.Minute
	}
	if c.ClickHouse.Database == "" {
		c.ClickHouse.Database = "ensemble"
	}
	if c.ClickHouse.Table == "" {
		c.ClickHouse.Table = "tap_events"
	}
	if c.ClickHouse.BatchSize == 0 {
		c.ClickHouse.BatchSize = 500
	}
	if c.ClickHouse.FlushInterval == 0 {
		c.ClickHouse.FlushInterval = 2 * time.Second
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.BasePath == "" {
		c.Server.BasePath = "/webhooks"
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 10 * time.Second
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 5 * time.Second
	}
	if c.Server.MaxBodySize == 0 {
		c.Server.MaxBodySize = 1 << 20
	}
	if c.Server.AdminReplayMaxLimit == 0 {
		c.Server.AdminReplayMaxLimit = defaultAdminReplayMaxLimit
	}
	if c.State.Backend == "" {
		c.State.Backend = "memory"
	}
	if c.State.SQLitePath == "" {
		c.State.SQLitePath = "tap-state.db"
	}
}

func (c Config) Validate() error {
	primary := strings.TrimSpace(c.Server.AdminToken)
	secondary := strings.TrimSpace(c.Server.AdminTokenSecondary)
	if primary == "" && secondary != "" {
		return fmt.Errorf("server.admin_token_secondary requires server.admin_token")
	}
	if primary != "" && secondary != "" && primary == secondary {
		return fmt.Errorf("server.admin_token and server.admin_token_secondary must differ")
	}
	if c.Server.AdminReplayMaxLimit <= 0 || c.Server.AdminReplayMaxLimit > maxAdminReplayMaxLimit {
		return fmt.Errorf("server.admin_replay_max_limit must be in range 1..%d", maxAdminReplayMaxLimit)
	}
	return nil
}

func Load(path string) (Config, error) {
	if path == "" {
		path = defaultConfigPath
	}

	k := koanf.New(".")

	if _, err := os.Stat(path); err == nil {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		expanded := os.ExpandEnv(string(raw))
		tmpFile, err := os.CreateTemp("", "tap-config-*.yaml")
		if err != nil {
			return Config{}, fmt.Errorf("create temp config: %w", err)
		}
		defer os.Remove(tmpFile.Name())
		if _, err := tmpFile.WriteString(expanded); err != nil {
			return Config{}, fmt.Errorf("write temp config: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			return Config{}, fmt.Errorf("close temp config: %w", err)
		}
		if err := k.Load(file.Provider(tmpFile.Name()), yaml.Parser()); err != nil {
			return Config{}, fmt.Errorf("load file config: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("stat config: %w", err)
	}

	if err := applyEnvOverrides(k); err != nil {
		return Config{}, fmt.Errorf("load env config: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

func applyEnvOverrides(k *koanf.Koanf) error {
	for _, kv := range os.Environ() {
		key, value, found := strings.Cut(kv, "=")
		if !found {
			continue
		}
		path, ok := envKeyToPath(key)
		if !ok {
			continue
		}
		if err := k.Set(path, value); err != nil {
			return fmt.Errorf("set %s: %w", path, err)
		}
	}
	return nil
}

func envKeyToPath(key string) (string, bool) {
	if !strings.HasPrefix(key, envPrefix) {
		return "", false
	}
	raw := strings.ToLower(strings.TrimPrefix(key, envPrefix))
	if raw == "" {
		return "", false
	}

	// Allow escaped underscores from old style (e.g. SUBJECT__PREFIX).
	raw = strings.ReplaceAll(raw, "__", "_")
	parts := strings.Split(raw, "_")
	if len(parts) < 2 {
		return "", false
	}

	switch parts[0] {
	case "providers":
		if len(parts) < 3 {
			return "", false
		}
		provider := parts[1]
		field := strings.Join(parts[2:], "_")
		return "providers." + provider + "." + field, true
	default:
		section := parts[0]
		field := strings.Join(parts[1:], "_")
		return section + "." + field, true
	}
}
