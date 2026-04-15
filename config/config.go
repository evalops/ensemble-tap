package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	defaultConfigPath            = "config.yaml"
	envPrefix                    = "TAP_"
	defaultNATSConnectTimeout    = 5 * time.Second
	defaultNATSReconnectWait     = 2 * time.Second
	defaultNATSMaxReconnects     = -1
	defaultNATSPublishTimeout    = 5 * time.Second
	defaultNATSPublishRetries    = 3
	defaultNATSPublishRetryDelay = 100 * time.Millisecond
	defaultNATSStreamReplicas    = 1
	defaultNATSStreamStorage     = "file"
	defaultNATSStreamDiscard     = "old"
	defaultNATSStreamCompression = "none"
	defaultClickHouseUser        = "default"
	defaultClickHouseDialTimeout = 5 * time.Second
	defaultClickHouseMaxOpen     = 4
	defaultClickHouseMaxIdle     = 2
	defaultClickHouseConnMaxLife = 30 * time.Minute
	defaultClickHouseBatchSize   = 500
	defaultClickHouseFlushEvery  = 2 * time.Second
	defaultClickHouseFetchBatch  = 100
	defaultClickHouseFetchWait   = 500 * time.Millisecond
	defaultClickHouseAckWait     = 30 * time.Second
	defaultClickHouseAckPending  = 1000
	defaultClickHouseInsertTO    = 10 * time.Second
	defaultClickHouseConsumer    = "tap_clickhouse_sink"
	defaultClickHouseRetention   = 365 * 24 * time.Hour
	defaultAdminReplayMaxLimit   = 2000
	maxAdminReplayMaxLimit       = 100000
	defaultAdminReplayJobTTL     = 24 * time.Hour
	defaultAdminReplayJobMax     = 512
	maxAdminReplayJobMax         = 100000
	defaultAdminReplayJobTimeout = 5 * time.Minute
	defaultAdminReplayConcurrent = 2
	maxAdminReplayConcurrent     = 100
	defaultAdminReplayStore      = "memory"
	defaultAdminReplaySQLitePath = "tap-admin-replay.db"
	defaultAdminReplayReasonMin  = 12
	defaultAdminReplayQueuedIP   = 100
	defaultAdminReplayQueuedTok  = 20
	maxAdminReplayQueuedLimit    = 100000
	defaultAdminRateLimitPerSec  = 5.0
	defaultAdminRateLimitBurst   = 20
)

type Config struct {
	Providers  map[string]ProviderConfig `koanf:"providers"`
	NATS       NATSConfig                `koanf:"nats"`
	ClickHouse ClickHouseConfig          `koanf:"clickhouse"`
	Vault      VaultConfig               `koanf:"vault"`
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
	PollMaxPages         int                             `koanf:"poll_max_pages"`
	PollMaxRequests      int                             `koanf:"poll_max_requests"`
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
	PollMaxPages        int           `koanf:"poll_max_pages"`
	PollMaxRequests     int           `koanf:"poll_max_requests"`
}

type NATSConfig struct {
	URL                  string        `koanf:"url"`
	Stream               string        `koanf:"stream"`
	SubjectPrefix        string        `koanf:"subject_prefix"`
	TenantScopedSubjects bool          `koanf:"tenant_scoped_subjects"`
	MaxAge               time.Duration `koanf:"max_age"`
	DedupWindow          time.Duration `koanf:"dedup_window"`
	ConnectTimeout       time.Duration `koanf:"connect_timeout"`
	ReconnectWait        time.Duration `koanf:"reconnect_wait"`
	MaxReconnects        int           `koanf:"max_reconnects"`
	PublishTimeout       time.Duration `koanf:"publish_timeout"`
	PublishMaxRetries    int           `koanf:"publish_max_retries"`
	PublishRetryBackoff  time.Duration `koanf:"publish_retry_backoff"`
	Username             string        `koanf:"username"`
	Password             string        `koanf:"password"`
	Token                string        `koanf:"token"`
	CredsFile            string        `koanf:"creds_file"`
	Secure               bool          `koanf:"secure"`
	InsecureSkipVerify   bool          `koanf:"insecure_skip_verify"`
	CAFile               string        `koanf:"ca_file"`
	CertFile             string        `koanf:"cert_file"`
	KeyFile              string        `koanf:"key_file"`
	StreamReplicas       int           `koanf:"stream_replicas"`
	StreamStorage        string        `koanf:"stream_storage"`
	StreamDiscard        string        `koanf:"stream_discard"`
	StreamMaxConsumers   int           `koanf:"stream_max_consumers"`
	StreamMaxMsgsPerSub  int64         `koanf:"stream_max_msgs_per_subject"`
	StreamCompression    string        `koanf:"stream_compression"`
	StreamAllowMsgTTL    bool          `koanf:"stream_allow_msg_ttl"`
	StreamMaxMsgs        int64         `koanf:"stream_max_msgs"`
	StreamMaxBytes       int64         `koanf:"stream_max_bytes"`
	StreamMaxMsgSize     int           `koanf:"stream_max_msg_size"`
}

type ClickHouseConfig struct {
	Addr                       string          `koanf:"addr"`
	Database                   string          `koanf:"database"`
	Table                      string          `koanf:"table"`
	Username                   string          `koanf:"username"`
	Password                   string          `koanf:"password"`
	Secure                     bool            `koanf:"secure"`
	InsecureSkipVerify         bool            `koanf:"insecure_skip_verify"`
	TLSServerName              string          `koanf:"tls_server_name"`
	CAFile                     string          `koanf:"ca_file"`
	CertFile                   string          `koanf:"cert_file"`
	KeyFile                    string          `koanf:"key_file"`
	DialTimeout                time.Duration   `koanf:"dial_timeout"`
	MaxOpenConns               int             `koanf:"max_open_conns"`
	MaxIdleConns               int             `koanf:"max_idle_conns"`
	ConnMaxLifetime            time.Duration   `koanf:"conn_max_lifetime"`
	BatchSize                  int             `koanf:"batch_size"`
	FlushInterval              time.Duration   `koanf:"flush_interval"`
	ConsumerName               string          `koanf:"consumer_name"`
	ConsumerFetchBatch         int             `koanf:"consumer_fetch_batch_size"`
	ConsumerFetchMaxWait       time.Duration   `koanf:"consumer_fetch_max_wait"`
	ConsumerAckWait            time.Duration   `koanf:"consumer_ack_wait"`
	ConsumerMaxAckPending      int             `koanf:"consumer_max_ack_pending"`
	ConsumerMaxDeliver         int             `koanf:"consumer_max_deliver"`
	ConsumerBackoff            []time.Duration `koanf:"consumer_backoff"`
	ConsumerMaxWaiting         int             `koanf:"consumer_max_waiting"`
	ConsumerMaxRequestMaxBytes int             `koanf:"consumer_max_request_max_bytes"`
	InsertTimeout              time.Duration   `koanf:"insert_timeout"`
	RetentionTTL               time.Duration   `koanf:"retention_ttl"`
}

type ServerConfig struct {
	Port                      int           `koanf:"port"`
	BasePath                  string        `koanf:"base_path"`
	ReadTimeout               time.Duration `koanf:"read_timeout"`
	WriteTimeout              time.Duration `koanf:"write_timeout"`
	MaxBodySize               int64         `koanf:"max_body_size"`
	AdminToken                string        `koanf:"admin_token"`
	AdminTokenSecondary       string        `koanf:"admin_token_secondary"`
	AdminTokenRead            string        `koanf:"admin_token_read"`
	AdminTokenReplay          string        `koanf:"admin_token_replay"`
	AdminTokenCancel          string        `koanf:"admin_token_cancel"`
	AdminReplayMaxLimit       int           `koanf:"admin_replay_max_limit"`
	AdminReplayJobTTL         time.Duration `koanf:"admin_replay_job_ttl"`
	AdminReplayJobMaxJobs     int           `koanf:"admin_replay_job_max_jobs"`
	AdminReplayJobTimeout     time.Duration `koanf:"admin_replay_job_timeout"`
	AdminReplayMaxConcurrent  int           `koanf:"admin_replay_max_concurrent_jobs"`
	AdminReplayStoreBackend   string        `koanf:"admin_replay_store_backend"`
	AdminReplaySQLitePath     string        `koanf:"admin_replay_sqlite_path"`
	AdminReplayRequireReason  bool          `koanf:"admin_replay_require_reason"`
	AdminReplayReasonMinLen   int           `koanf:"admin_replay_reason_min_length"`
	AdminReplayMaxQueuedPerIP int           `koanf:"admin_replay_max_queued_per_ip"`
	AdminReplayMaxQueuedToken int           `koanf:"admin_replay_max_queued_per_token"`
	AdminRateLimitPerSec      float64       `koanf:"admin_rate_limit_per_sec"`
	AdminRateLimitBurst       int           `koanf:"admin_rate_limit_burst"`
	AdminAllowedCIDRs         []string      `koanf:"admin_allowed_cidrs"`
	AdminMTLSRequired         bool          `koanf:"admin_mtls_required"`
	AdminMTLSClientCertHeader string        `koanf:"admin_mtls_client_cert_header"`
}

type StateConfig struct {
	Backend    string `koanf:"backend"`
	SQLitePath string `koanf:"sqlite_path"`
}

type VaultConfig struct {
	Address             string `koanf:"address"`
	Namespace           string `koanf:"namespace"`
	AuthMethod          string `koanf:"auth_method"`
	Token               string `koanf:"token"`
	TokenFile           string `koanf:"token_file"`
	KubernetesRole      string `koanf:"kubernetes_role"`
	KubernetesMountPath string `koanf:"kubernetes_mount_path"`
	KubernetesJWTFile   string `koanf:"kubernetes_jwt_file"`
}

func (c *Config) ApplyDefaults() {
	if c.Providers == nil {
		c.Providers = make(map[string]ProviderConfig)
	}
	if c.NATS.URL == "" {
		c.NATS.URL = "nats://localhost:4222"
	}
	if c.NATS.Stream == "" {
		c.NATS.Stream = "SIPHON"
	}
	if c.NATS.SubjectPrefix == "" {
		c.NATS.SubjectPrefix = "siphon.tap"
	}
	if c.NATS.MaxAge == 0 {
		c.NATS.MaxAge = 7 * 24 * time.Hour
	}
	if c.NATS.DedupWindow == 0 {
		c.NATS.DedupWindow = 2 * time.Minute
	}
	if c.NATS.ConnectTimeout == 0 {
		c.NATS.ConnectTimeout = defaultNATSConnectTimeout
	}
	if c.NATS.ReconnectWait == 0 {
		c.NATS.ReconnectWait = defaultNATSReconnectWait
	}
	if c.NATS.MaxReconnects == 0 {
		c.NATS.MaxReconnects = defaultNATSMaxReconnects
	}
	if c.NATS.PublishTimeout == 0 {
		c.NATS.PublishTimeout = defaultNATSPublishTimeout
	}
	if c.NATS.PublishMaxRetries == 0 {
		c.NATS.PublishMaxRetries = defaultNATSPublishRetries
	}
	if c.NATS.PublishRetryBackoff == 0 {
		c.NATS.PublishRetryBackoff = defaultNATSPublishRetryDelay
	}
	if c.NATS.StreamReplicas == 0 {
		c.NATS.StreamReplicas = defaultNATSStreamReplicas
	}
	if strings.TrimSpace(c.NATS.StreamStorage) == "" {
		c.NATS.StreamStorage = defaultNATSStreamStorage
	}
	if strings.TrimSpace(c.NATS.StreamDiscard) == "" {
		c.NATS.StreamDiscard = defaultNATSStreamDiscard
	}
	if strings.TrimSpace(c.NATS.StreamCompression) == "" {
		c.NATS.StreamCompression = defaultNATSStreamCompression
	}
	if c.ClickHouse.Database == "" {
		c.ClickHouse.Database = "siphon"
	}
	if c.ClickHouse.Table == "" {
		c.ClickHouse.Table = "tap_events"
	}
	if strings.TrimSpace(c.ClickHouse.Username) == "" {
		c.ClickHouse.Username = defaultClickHouseUser
	}
	if c.ClickHouse.DialTimeout == 0 {
		c.ClickHouse.DialTimeout = defaultClickHouseDialTimeout
	}
	if c.ClickHouse.MaxOpenConns == 0 {
		c.ClickHouse.MaxOpenConns = defaultClickHouseMaxOpen
	}
	if c.ClickHouse.MaxIdleConns == 0 {
		c.ClickHouse.MaxIdleConns = defaultClickHouseMaxIdle
	}
	if c.ClickHouse.ConnMaxLifetime == 0 {
		c.ClickHouse.ConnMaxLifetime = defaultClickHouseConnMaxLife
	}
	if c.ClickHouse.BatchSize == 0 {
		c.ClickHouse.BatchSize = defaultClickHouseBatchSize
	}
	if c.ClickHouse.FlushInterval == 0 {
		c.ClickHouse.FlushInterval = defaultClickHouseFlushEvery
	}
	if strings.TrimSpace(c.ClickHouse.ConsumerName) == "" {
		c.ClickHouse.ConsumerName = defaultClickHouseConsumer
	}
	if c.ClickHouse.ConsumerFetchBatch == 0 {
		c.ClickHouse.ConsumerFetchBatch = defaultClickHouseFetchBatch
	}
	if c.ClickHouse.ConsumerFetchMaxWait == 0 {
		c.ClickHouse.ConsumerFetchMaxWait = defaultClickHouseFetchWait
	}
	if c.ClickHouse.ConsumerAckWait == 0 {
		c.ClickHouse.ConsumerAckWait = defaultClickHouseAckWait
	}
	if c.ClickHouse.ConsumerMaxAckPending == 0 {
		c.ClickHouse.ConsumerMaxAckPending = defaultClickHouseAckPending
	}
	if c.ClickHouse.InsertTimeout == 0 {
		c.ClickHouse.InsertTimeout = defaultClickHouseInsertTO
	}
	if c.ClickHouse.RetentionTTL == 0 {
		c.ClickHouse.RetentionTTL = defaultClickHouseRetention
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
	if c.Server.AdminReplayJobTTL == 0 {
		c.Server.AdminReplayJobTTL = defaultAdminReplayJobTTL
	}
	if c.Server.AdminReplayJobMaxJobs == 0 {
		c.Server.AdminReplayJobMaxJobs = defaultAdminReplayJobMax
	}
	if c.Server.AdminReplayJobTimeout == 0 {
		c.Server.AdminReplayJobTimeout = defaultAdminReplayJobTimeout
	}
	if c.Server.AdminReplayMaxConcurrent == 0 {
		c.Server.AdminReplayMaxConcurrent = defaultAdminReplayConcurrent
	}
	if strings.TrimSpace(c.Server.AdminReplayStoreBackend) == "" {
		c.Server.AdminReplayStoreBackend = defaultAdminReplayStore
	}
	if strings.TrimSpace(c.Server.AdminReplaySQLitePath) == "" {
		c.Server.AdminReplaySQLitePath = defaultAdminReplaySQLitePath
	}
	if c.Server.AdminReplayReasonMinLen == 0 {
		c.Server.AdminReplayReasonMinLen = defaultAdminReplayReasonMin
	}
	if c.Server.AdminReplayMaxQueuedPerIP == 0 {
		c.Server.AdminReplayMaxQueuedPerIP = defaultAdminReplayQueuedIP
	}
	if c.Server.AdminReplayMaxQueuedToken == 0 {
		c.Server.AdminReplayMaxQueuedToken = defaultAdminReplayQueuedTok
	}
	if c.Server.AdminRateLimitPerSec == 0 {
		c.Server.AdminRateLimitPerSec = defaultAdminRateLimitPerSec
	}
	if c.Server.AdminRateLimitBurst == 0 {
		c.Server.AdminRateLimitBurst = defaultAdminRateLimitBurst
	}
	if strings.TrimSpace(c.Server.AdminMTLSClientCertHeader) == "" {
		c.Server.AdminMTLSClientCertHeader = "X-Forwarded-Client-Cert"
	}
	if c.State.Backend == "" {
		c.State.Backend = "memory"
	}
	if c.State.SQLitePath == "" {
		c.State.SQLitePath = "tap-state.db"
	}
	if strings.TrimSpace(c.Vault.Address) == "" {
		c.Vault.Address = strings.TrimSpace(os.Getenv("VAULT_ADDR"))
	}
	if strings.TrimSpace(c.Vault.AuthMethod) == "" {
		c.Vault.AuthMethod = "kubernetes"
	}
	if strings.TrimSpace(c.Vault.KubernetesMountPath) == "" {
		c.Vault.KubernetesMountPath = "kubernetes"
	}
	if strings.TrimSpace(c.Vault.KubernetesJWTFile) == "" {
		c.Vault.KubernetesJWTFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
}

func (c Config) Validate() error {
	if err := validateNATSConfig(c.NATS); err != nil {
		return err
	}
	if err := validateClickHouseConfig(c.ClickHouse); err != nil {
		return err
	}

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
	if c.Server.AdminReplayJobTTL <= 0 {
		return fmt.Errorf("server.admin_replay_job_ttl must be greater than 0")
	}
	if c.Server.AdminReplayJobMaxJobs <= 0 || c.Server.AdminReplayJobMaxJobs > maxAdminReplayJobMax {
		return fmt.Errorf("server.admin_replay_job_max_jobs must be in range 1..%d", maxAdminReplayJobMax)
	}
	if c.Server.AdminReplayJobTimeout <= 0 {
		return fmt.Errorf("server.admin_replay_job_timeout must be greater than 0")
	}
	if c.Server.AdminReplayMaxConcurrent <= 0 || c.Server.AdminReplayMaxConcurrent > maxAdminReplayConcurrent {
		return fmt.Errorf("server.admin_replay_max_concurrent_jobs must be in range 1..%d", maxAdminReplayConcurrent)
	}
	replayStoreBackend := strings.ToLower(strings.TrimSpace(c.Server.AdminReplayStoreBackend))
	switch replayStoreBackend {
	case "memory", "sqlite":
	default:
		return fmt.Errorf("server.admin_replay_store_backend must be one of memory|sqlite")
	}
	if replayStoreBackend == "sqlite" && strings.TrimSpace(c.Server.AdminReplaySQLitePath) == "" {
		return fmt.Errorf("server.admin_replay_sqlite_path must not be empty when server.admin_replay_store_backend=sqlite")
	}
	if c.Server.AdminReplayReasonMinLen <= 0 {
		return fmt.Errorf("server.admin_replay_reason_min_length must be greater than 0")
	}
	if c.Server.AdminReplayMaxQueuedPerIP < 0 || c.Server.AdminReplayMaxQueuedPerIP > maxAdminReplayQueuedLimit {
		return fmt.Errorf("server.admin_replay_max_queued_per_ip must be in range 0..%d", maxAdminReplayQueuedLimit)
	}
	if c.Server.AdminReplayMaxQueuedToken < 0 || c.Server.AdminReplayMaxQueuedToken > maxAdminReplayQueuedLimit {
		return fmt.Errorf("server.admin_replay_max_queued_per_token must be in range 0..%d", maxAdminReplayQueuedLimit)
	}
	if c.Server.AdminRateLimitPerSec <= 0 {
		return fmt.Errorf("server.admin_rate_limit_per_sec must be greater than 0")
	}
	if c.Server.AdminRateLimitBurst <= 0 {
		return fmt.Errorf("server.admin_rate_limit_burst must be greater than 0")
	}
	for _, cidr := range c.Server.AdminAllowedCIDRs {
		raw := strings.TrimSpace(cidr)
		if raw == "" {
			continue
		}
		if _, _, err := net.ParseCIDR(raw); err != nil {
			return fmt.Errorf("server.admin_allowed_cidrs contains invalid CIDR %q", raw)
		}
	}
	if strings.TrimSpace(c.Server.AdminMTLSClientCertHeader) == "" {
		return fmt.Errorf("server.admin_mtls_client_cert_header must not be empty")
	}
	if err := validateProviders(c.Providers); err != nil {
		return err
	}
	return nil
}

func validateNATSConfig(cfg NATSConfig) error {
	rawURL := strings.TrimSpace(cfg.URL)
	if rawURL == "" {
		return fmt.Errorf("nats.url must not be empty")
	}
	natsEndpoints := strings.Split(rawURL, ",")
	for i, endpointRaw := range natsEndpoints {
		endpoint := strings.TrimSpace(endpointRaw)
		if endpoint == "" {
			return fmt.Errorf("nats.url contains empty endpoint at index %d", i)
		}
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("nats.url endpoint %q is invalid: %w", endpoint, err)
		}
		scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
		switch scheme {
		case "nats", "tls", "ws", "wss":
		default:
			return fmt.Errorf("nats.url endpoint %q has unsupported scheme %q (expected one of nats|tls|ws|wss)", endpoint, scheme)
		}
		host := strings.TrimSpace(u.Hostname())
		if host == "" {
			return fmt.Errorf("nats.url endpoint %q must include a host", endpoint)
		}
		if scheme == "nats" || scheme == "tls" {
			if path := strings.TrimSpace(u.EscapedPath()); path != "" && path != "/" {
				return fmt.Errorf("nats.url endpoint %q must not include a path for %s scheme", endpoint, scheme)
			}
		}
		port := strings.TrimSpace(u.Port())
		if port != "" {
			if err := validatePortNumber(port); err != nil {
				return fmt.Errorf("nats.url endpoint %q has invalid port: %w", endpoint, err)
			}
		}
	}
	if strings.TrimSpace(cfg.Stream) == "" {
		return fmt.Errorf("nats.stream must not be empty")
	}
	subjectPrefix := strings.TrimSpace(cfg.SubjectPrefix)
	if subjectPrefix == "" {
		return fmt.Errorf("nats.subject_prefix must not be empty")
	}
	if strings.ContainsAny(subjectPrefix, " \t\r\n") {
		return fmt.Errorf("nats.subject_prefix must not contain whitespace")
	}
	if strings.Contains(subjectPrefix, "*") || strings.Contains(subjectPrefix, ">") {
		return fmt.Errorf("nats.subject_prefix must not contain wildcard tokens")
	}
	if cfg.MaxAge <= 0 {
		return fmt.Errorf("nats.max_age must be greater than 0")
	}
	if cfg.DedupWindow <= 0 {
		return fmt.Errorf("nats.dedup_window must be greater than 0")
	}
	if cfg.DedupWindow > cfg.MaxAge {
		return fmt.Errorf("nats.dedup_window must be less than or equal to nats.max_age")
	}
	if cfg.ConnectTimeout <= 0 {
		return fmt.Errorf("nats.connect_timeout must be greater than 0")
	}
	if cfg.ReconnectWait <= 0 {
		return fmt.Errorf("nats.reconnect_wait must be greater than 0")
	}
	if cfg.MaxReconnects < -1 {
		return fmt.Errorf("nats.max_reconnects must be greater than or equal to -1")
	}
	if cfg.PublishTimeout <= 0 {
		return fmt.Errorf("nats.publish_timeout must be greater than 0")
	}
	if cfg.PublishMaxRetries < 0 {
		return fmt.Errorf("nats.publish_max_retries must be greater than or equal to 0")
	}
	if cfg.PublishMaxRetries > 1000 {
		return fmt.Errorf("nats.publish_max_retries must be less than or equal to 1000")
	}
	if cfg.PublishRetryBackoff <= 0 {
		return fmt.Errorf("nats.publish_retry_backoff must be greater than 0")
	}
	username := strings.TrimSpace(cfg.Username)
	password := strings.TrimSpace(cfg.Password)
	token := strings.TrimSpace(cfg.Token)
	creds := strings.TrimSpace(cfg.CredsFile)
	caFile := strings.TrimSpace(cfg.CAFile)
	certFile := strings.TrimSpace(cfg.CertFile)
	keyFile := strings.TrimSpace(cfg.KeyFile)
	if password != "" && username == "" {
		return fmt.Errorf("nats.password requires nats.username")
	}
	if token != "" && (username != "" || password != "") {
		return fmt.Errorf("nats.token cannot be combined with nats.username or nats.password")
	}
	if creds != "" && (token != "" || username != "" || password != "") {
		return fmt.Errorf("nats.creds_file cannot be combined with nats.token, nats.username, or nats.password")
	}
	if cfg.InsecureSkipVerify && !cfg.Secure {
		return fmt.Errorf("nats.insecure_skip_verify requires nats.secure=true")
	}
	if (caFile != "" || certFile != "" || keyFile != "") && !cfg.Secure {
		return fmt.Errorf("nats.ca_file, nats.cert_file, and nats.key_file require nats.secure=true")
	}
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("nats.cert_file and nats.key_file must be configured together")
	}
	if cfg.StreamReplicas <= 0 {
		return fmt.Errorf("nats.stream_replicas must be greater than 0")
	}
	streamStorage := strings.ToLower(strings.TrimSpace(cfg.StreamStorage))
	switch streamStorage {
	case "file", "memory":
	default:
		return fmt.Errorf("nats.stream_storage must be one of file|memory")
	}
	streamDiscard := strings.ToLower(strings.TrimSpace(cfg.StreamDiscard))
	switch streamDiscard {
	case "old", "new":
	default:
		return fmt.Errorf("nats.stream_discard must be one of old|new")
	}
	if cfg.StreamMaxConsumers < 0 {
		return fmt.Errorf("nats.stream_max_consumers must be greater than or equal to 0")
	}
	if cfg.StreamMaxMsgsPerSub < 0 {
		return fmt.Errorf("nats.stream_max_msgs_per_subject must be greater than or equal to 0")
	}
	streamCompression := strings.ToLower(strings.TrimSpace(cfg.StreamCompression))
	switch streamCompression {
	case "", "none", "s2":
	default:
		return fmt.Errorf("nats.stream_compression must be one of none|s2")
	}
	if cfg.StreamMaxMsgs < 0 {
		return fmt.Errorf("nats.stream_max_msgs must be greater than or equal to 0")
	}
	if cfg.StreamMaxBytes < 0 {
		return fmt.Errorf("nats.stream_max_bytes must be greater than or equal to 0")
	}
	if cfg.StreamMaxMsgSize < 0 {
		return fmt.Errorf("nats.stream_max_msg_size must be greater than or equal to 0")
	}
	if cfg.StreamMaxMsgSize > math.MaxInt32 {
		return fmt.Errorf("nats.stream_max_msg_size must be less than or equal to %d", math.MaxInt32)
	}
	return nil
}

func validateClickHouseConfig(cfg ClickHouseConfig) error {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil
	}
	tlsServerName := strings.TrimSpace(cfg.TLSServerName)
	caFile := strings.TrimSpace(cfg.CAFile)
	certFile := strings.TrimSpace(cfg.CertFile)
	keyFile := strings.TrimSpace(cfg.KeyFile)
	if strings.TrimSpace(cfg.Database) == "" {
		return fmt.Errorf("clickhouse.database must not be empty when clickhouse.addr is configured")
	}
	if strings.TrimSpace(cfg.Table) == "" {
		return fmt.Errorf("clickhouse.table must not be empty when clickhouse.addr is configured")
	}
	for _, raw := range strings.Split(addr, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			return fmt.Errorf("clickhouse.addr must not contain empty entries")
		}
		host, port, err := net.SplitHostPort(entry)
		if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
			return fmt.Errorf("clickhouse.addr entry %q must be host:port", entry)
		}
		if err := validatePortNumber(port); err != nil {
			return fmt.Errorf("clickhouse.addr entry %q has invalid port: %w", entry, err)
		}
	}
	if strings.TrimSpace(cfg.Username) == "" {
		return fmt.Errorf("clickhouse.username must not be empty when clickhouse.addr is configured")
	}
	if cfg.InsecureSkipVerify && !cfg.Secure {
		return fmt.Errorf("clickhouse.insecure_skip_verify requires clickhouse.secure=true")
	}
	if (tlsServerName != "" || caFile != "" || certFile != "" || keyFile != "") && !cfg.Secure {
		return fmt.Errorf("clickhouse.tls_server_name, clickhouse.ca_file, clickhouse.cert_file, and clickhouse.key_file require clickhouse.secure=true")
	}
	if strings.ContainsAny(tlsServerName, " \t\r\n") {
		return fmt.Errorf("clickhouse.tls_server_name must not contain whitespace")
	}
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("clickhouse.cert_file and clickhouse.key_file must be configured together")
	}
	if cfg.DialTimeout <= 0 {
		return fmt.Errorf("clickhouse.dial_timeout must be greater than 0")
	}
	if cfg.MaxOpenConns <= 0 {
		return fmt.Errorf("clickhouse.max_open_conns must be greater than 0")
	}
	if cfg.MaxIdleConns < 0 {
		return fmt.Errorf("clickhouse.max_idle_conns must be greater than or equal to 0")
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return fmt.Errorf("clickhouse.max_idle_conns must be less than or equal to clickhouse.max_open_conns")
	}
	if cfg.ConnMaxLifetime < 0 {
		return fmt.Errorf("clickhouse.conn_max_lifetime must be greater than or equal to 0")
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("clickhouse.batch_size must be greater than 0")
	}
	if cfg.FlushInterval <= 0 {
		return fmt.Errorf("clickhouse.flush_interval must be greater than 0")
	}
	consumerName := strings.TrimSpace(cfg.ConsumerName)
	if consumerName == "" {
		return fmt.Errorf("clickhouse.consumer_name must not be empty when clickhouse.addr is configured")
	}
	if strings.ContainsAny(consumerName, " \t\r\n") {
		return fmt.Errorf("clickhouse.consumer_name must not contain whitespace")
	}
	if cfg.ConsumerFetchBatch <= 0 {
		return fmt.Errorf("clickhouse.consumer_fetch_batch_size must be greater than 0")
	}
	if cfg.ConsumerFetchMaxWait <= 0 {
		return fmt.Errorf("clickhouse.consumer_fetch_max_wait must be greater than 0")
	}
	if cfg.ConsumerAckWait <= 0 {
		return fmt.Errorf("clickhouse.consumer_ack_wait must be greater than 0")
	}
	if cfg.ConsumerFetchMaxWait >= cfg.ConsumerAckWait {
		return fmt.Errorf("clickhouse.consumer_fetch_max_wait must be less than clickhouse.consumer_ack_wait")
	}
	if cfg.ConsumerMaxAckPending <= 0 {
		return fmt.Errorf("clickhouse.consumer_max_ack_pending must be greater than 0")
	}
	if cfg.ConsumerMaxDeliver < -1 {
		return fmt.Errorf("clickhouse.consumer_max_deliver must be greater than or equal to -1")
	}
	if cfg.ConsumerMaxWaiting < 0 {
		return fmt.Errorf("clickhouse.consumer_max_waiting must be greater than or equal to 0")
	}
	if cfg.ConsumerMaxRequestMaxBytes < 0 {
		return fmt.Errorf("clickhouse.consumer_max_request_max_bytes must be greater than or equal to 0")
	}
	if len(cfg.ConsumerBackoff) > 0 {
		prev := time.Duration(0)
		for i, backoff := range cfg.ConsumerBackoff {
			if backoff <= 0 {
				return fmt.Errorf("clickhouse.consumer_backoff[%d] must be greater than 0", i)
			}
			if i > 0 && backoff < prev {
				return fmt.Errorf("clickhouse.consumer_backoff must be non-decreasing")
			}
			prev = backoff
		}
		if cfg.ConsumerMaxDeliver > 0 && cfg.ConsumerMaxDeliver != len(cfg.ConsumerBackoff) {
			return fmt.Errorf("clickhouse.consumer_max_deliver must equal len(clickhouse.consumer_backoff) when both are configured")
		}
	}
	if cfg.InsertTimeout <= 0 {
		return fmt.Errorf("clickhouse.insert_timeout must be greater than 0")
	}
	if cfg.RetentionTTL <= 0 {
		return fmt.Errorf("clickhouse.retention_ttl must be greater than 0")
	}
	if cfg.FlushInterval >= cfg.ConsumerAckWait {
		return fmt.Errorf("clickhouse.flush_interval must be less than clickhouse.consumer_ack_wait")
	}
	if cfg.InsertTimeout >= cfg.ConsumerAckWait {
		return fmt.Errorf("clickhouse.insert_timeout must be less than clickhouse.consumer_ack_wait")
	}
	if cfg.InsertTimeout+cfg.FlushInterval >= cfg.ConsumerAckWait {
		return fmt.Errorf("clickhouse.insert_timeout + clickhouse.flush_interval must be less than clickhouse.consumer_ack_wait")
	}
	return nil
}

func validatePortNumber(raw string) error {
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("port %q must be numeric", raw)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port %d must be in range 1..65535", port)
	}
	return nil
}

func validateProviders(providers map[string]ProviderConfig) error {
	for name, providerCfg := range providers {
		providerName := strings.ToLower(strings.TrimSpace(name))
		if providerName == "" {
			return fmt.Errorf("providers contains an empty provider key")
		}

		mode := normalizeProviderMode(providerCfg.Mode)
		if modeContainsWebhook(mode) {
			if err := validateWebhookProvider(providerName, providerCfg); err != nil {
				return err
			}
		}
		if modeContainsPoll(mode) {
			if err := validatePollProvider(providerName, providerCfg); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeProviderMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	mode = strings.ReplaceAll(mode, " ", "")
	return mode
}

func modeContainsWebhook(mode string) bool {
	mode = normalizeProviderMode(mode)
	if mode == "" {
		// Webhook ingress accepts configured providers even without explicit mode.
		return true
	}
	return strings.Contains(mode, "webhook")
}

func modeContainsPoll(mode string) bool {
	mode = normalizeProviderMode(mode)
	return strings.Contains(mode, "poll")
}

func validateWebhookProvider(providerName string, providerCfg ProviderConfig) error {
	hasWebhookSecret := func(cfg ProviderConfig) bool {
		if providerName == "hubspot" {
			return strings.TrimSpace(cfg.Secret) != "" || strings.TrimSpace(cfg.ClientSecret) != ""
		}
		return strings.TrimSpace(cfg.Secret) != ""
	}
	if hasWebhookSecret(providerCfg) {
		return nil
	}
	for tenantKey := range providerCfg.Tenants {
		tenantCfg := ApplyProviderTenant(providerCfg, tenantKey)
		if hasWebhookSecret(tenantCfg) {
			return nil
		}
	}
	if providerName == "hubspot" {
		return fmt.Errorf("providers.%s webhook mode requires secret or client_secret (base or tenant override)", providerName)
	}
	return fmt.Errorf("providers.%s webhook mode requires secret (base or tenant override)", providerName)
}

func validatePollProvider(providerName string, providerCfg ProviderConfig) error {
	targets := buildPollTargetsForValidation(providerCfg)
	if len(targets) == 0 {
		return fmt.Errorf("providers.%s poll mode has no poll targets with credentials", providerName)
	}

	for _, target := range targets {
		if err := validatePollFetchBounds(target); err != nil {
			targetScope := "base"
			if tenantID := strings.TrimSpace(target.TenantID); tenantID != "" {
				targetScope = fmt.Sprintf("tenant %q", tenantID)
			}
			return fmt.Errorf("providers.%s poll target %s is invalid: %w", providerName, targetScope, err)
		}
		var err error
		switch providerName {
		case "hubspot":
			err = validateHubSpotPollTarget(target)
		case "salesforce":
			err = validateSalesforcePollTarget(target)
		case "quickbooks":
			err = validateQuickBooksPollTarget(target)
		case "notion":
			err = validateNotionPollTarget(target)
		default:
			return fmt.Errorf("providers.%s poll mode is not supported", providerName)
		}
		if err == nil {
			continue
		}
		targetScope := "base"
		if tenantID := strings.TrimSpace(target.TenantID); tenantID != "" {
			targetScope = fmt.Sprintf("tenant %q", tenantID)
		}
		return fmt.Errorf("providers.%s poll target %s is invalid: %w", providerName, targetScope, err)
	}
	return nil
}

func validatePollFetchBounds(cfg ProviderConfig) error {
	if cfg.PollMaxPages < 0 {
		return fmt.Errorf("poll_max_pages must be greater than or equal to 0")
	}
	if cfg.PollMaxRequests < 0 {
		return fmt.Errorf("poll_max_requests must be greater than or equal to 0")
	}
	return nil
}

func buildPollTargetsForValidation(base ProviderConfig) []ProviderConfig {
	targets := make([]ProviderConfig, 0)
	seen := map[string]struct{}{}

	addTarget := func(candidate ProviderConfig) {
		tenantID := strings.TrimSpace(candidate.TenantID)
		key := tenantID
		if key == "" {
			key = "__default__"
		}
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, candidate)
	}

	if len(base.Tenants) == 0 || hasBasePollCredentials(base) {
		addTarget(ApplyProviderTenant(base, strings.TrimSpace(base.TenantID)))
	}
	if len(base.Tenants) == 0 {
		return targets
	}

	tenantKeys := make([]string, 0, len(base.Tenants))
	for tenantKey := range base.Tenants {
		tenantKeys = append(tenantKeys, tenantKey)
	}
	sort.Strings(tenantKeys)
	for _, tenantKey := range tenantKeys {
		addTarget(ApplyProviderTenant(base, tenantKey))
	}
	return targets
}

func hasBasePollCredentials(cfg ProviderConfig) bool {
	return strings.TrimSpace(cfg.AccessToken) != "" ||
		strings.TrimSpace(cfg.APIKey) != "" ||
		strings.TrimSpace(cfg.Secret) != "" ||
		strings.TrimSpace(cfg.RefreshToken) != ""
}

func validateHubSpotPollTarget(cfg ProviderConfig) error {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("base_url is required")
	}
	if strings.TrimSpace(cfg.AccessToken) == "" && strings.TrimSpace(cfg.APIKey) == "" {
		return fmt.Errorf("access_token or api_key is required")
	}
	return nil
}

func validateSalesforcePollTarget(cfg ProviderConfig) error {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("base_url is required")
	}
	if strings.TrimSpace(cfg.AccessToken) == "" && strings.TrimSpace(cfg.Secret) == "" {
		return fmt.Errorf("access_token or secret is required")
	}
	return nil
}

func validateQuickBooksPollTarget(cfg ProviderConfig) error {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("base_url is required")
	}
	if strings.TrimSpace(cfg.RealmID) == "" {
		return fmt.Errorf("realm_id is required")
	}
	if strings.TrimSpace(cfg.AccessToken) == "" && strings.TrimSpace(cfg.Secret) == "" {
		return fmt.Errorf("access_token or secret is required")
	}
	return nil
}

func validateNotionPollTarget(cfg ProviderConfig) error {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("base_url is required")
	}
	if strings.TrimSpace(cfg.AccessToken) == "" && strings.TrimSpace(cfg.Secret) == "" {
		return fmt.Errorf("access_token or secret is required")
	}
	return nil
}

func Load(path string) (Config, error) {
	if path == "" {
		path = defaultConfigPath
	}

	k := koanf.New(".")

	if _, err := os.Stat(path); err == nil {
		// #nosec G304 -- config path is an intentional operator-controlled CLI input.
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
	if err := cfg.resolveVaultReferences(); err != nil {
		return Config{}, fmt.Errorf("resolve vault references: %w", err)
	}
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
