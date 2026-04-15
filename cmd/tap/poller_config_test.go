package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/evalops/siphon/config"
	pollproviders "github.com/evalops/siphon/internal/poller/providers"
)

func TestModeContainsPoll(t *testing.T) {
	tests := []struct {
		mode string
		want bool
	}{
		{mode: "poll", want: true},
		{mode: "webhook+poll", want: true},
		{mode: "webhook,poll", want: true},
		{mode: "webhook", want: false},
		{mode: "cdc", want: false},
	}
	for _, tt := range tests {
		if got := modeContainsPoll(tt.mode); got != tt.want {
			t.Fatalf("modeContainsPoll(%q) = %v, want %v", tt.mode, got, tt.want)
		}
	}
}

func TestFetcherForProvider(t *testing.T) {
	hubspot := fetcherForProvider("hubspot", config.ProviderConfig{APIKey: "api-key", BaseURL: "https://api.hubapi.com", Objects: []string{"deals"}})
	if _, ok := hubspot.(*pollproviders.HubSpotFetcher); !ok {
		t.Fatalf("expected hubspot fetcher type, got %T", hubspot)
	}

	salesforce := fetcherForProvider("salesforce", config.ProviderConfig{AccessToken: "tok", BaseURL: "https://sf.example.com"})
	if _, ok := salesforce.(*pollproviders.SalesforceFetcher); !ok {
		t.Fatalf("expected salesforce fetcher type, got %T", salesforce)
	}

	quickbooks := fetcherForProvider("quickbooks", config.ProviderConfig{Secret: "tok", BaseURL: "https://quickbooks.api.intuit.com", RealmID: "realm-1"})
	if _, ok := quickbooks.(*pollproviders.QuickBooksFetcher); !ok {
		t.Fatalf("expected quickbooks fetcher type, got %T", quickbooks)
	}

	notion := fetcherForProvider("notion", config.ProviderConfig{Secret: "tok", BaseURL: "https://api.notion.com"})
	if _, ok := notion.(*pollproviders.NotionFetcher); !ok {
		t.Fatalf("expected notion fetcher type, got %T", notion)
	}

	if unknown := fetcherForProvider("stripe", config.ProviderConfig{}); unknown != nil {
		t.Fatalf("expected nil fetcher for unsupported provider, got %T", unknown)
	}
}

func TestOpenPollStores(t *testing.T) {
	cp, snap, closer, err := openPollStores(config.StateConfig{Backend: "memory"})
	if err != nil {
		t.Fatalf("open memory stores: %v", err)
	}
	if closer != nil {
		t.Fatalf("memory backend should not return closer")
	}
	if err := cp.Set("hubspot", "cp"); err != nil {
		t.Fatalf("set memory checkpoint: %v", err)
	}
	if got, ok := cp.Get("hubspot"); !ok || got != "cp" {
		t.Fatalf("unexpected memory checkpoint: %q %v", got, ok)
	}
	if err := snap.Put("hubspot", "deal", "1", map[string]any{"stage": "open"}); err != nil {
		t.Fatalf("put memory snapshot: %v", err)
	}

	sqlitePath := filepath.Join(t.TempDir(), "state.db")
	cp2, _, closer2, err := openPollStores(config.StateConfig{Backend: "sqlite", SQLitePath: sqlitePath})
	if err != nil {
		t.Fatalf("open sqlite stores: %v", err)
	}
	defer closer2.Close()
	if err := cp2.Set("hubspot", "cp2"); err != nil {
		t.Fatalf("set sqlite checkpoint: %v", err)
	}
	if got, ok := cp2.Get("hubspot"); !ok || got != "cp2" {
		t.Fatalf("unexpected sqlite checkpoint: %q %v", got, ok)
	}

	if _, _, _, err := openPollStores(config.StateConfig{Backend: "unknown"}); err == nil {
		t.Fatalf("expected unsupported backend error")
	}
}

func TestBuildPollTargetsBaseOnly(t *testing.T) {
	base := config.ProviderConfig{
		TenantID: "tenant-base",
	}
	targets := buildPollTargets(base)
	if len(targets) != 1 {
		t.Fatalf("expected one base target, got %d", len(targets))
	}
	if targets[0].TenantID != "tenant-base" {
		t.Fatalf("unexpected tenant id: %q", targets[0].TenantID)
	}
}

func TestBuildPollTargetsTenantOverridesWithoutBaseCredentials(t *testing.T) {
	base := config.ProviderConfig{
		PollInterval:        60 * time.Second,
		PollRateLimitPerSec: 4.0,
		PollBurst:           1,
		PollFailureBudget:   5,
		PollCircuitBreak:    30 * time.Second,
		PollJitterRatio:     0.1,
		Tenants: map[string]config.ProviderTenantConfig{
			"tenant-b": {AccessToken: "token-b", PollInterval: 30 * time.Second, PollRateLimitPerSec: 2.0, PollBurst: 2, PollFailureBudget: 4, PollCircuitBreak: 20 * time.Second, PollJitterRatio: 0.2},
			"tenant-a": {AccessToken: "token-a", PollInterval: 15 * time.Second, PollRateLimitPerSec: 8.0, PollBurst: 4, PollFailureBudget: 7, PollCircuitBreak: 45 * time.Second, PollJitterRatio: 0.35},
		},
	}

	targets := buildPollTargets(base)
	if len(targets) != 2 {
		t.Fatalf("expected two tenant targets, got %d", len(targets))
	}
	if targets[0].TenantID != "tenant-a" || targets[0].AccessToken != "token-a" {
		t.Fatalf("unexpected first target: %+v", targets[0])
	}
	if targets[0].PollInterval != 15*time.Second || targets[0].PollRateLimitPerSec != 8.0 || targets[0].PollBurst != 4 {
		t.Fatalf("unexpected tenant-a poll settings: %+v", targets[0])
	}
	if targets[0].PollFailureBudget != 7 || targets[0].PollCircuitBreak != 45*time.Second || targets[0].PollJitterRatio != 0.35 {
		t.Fatalf("unexpected tenant-a resilience settings: %+v", targets[0])
	}
	if targets[1].TenantID != "tenant-b" || targets[1].AccessToken != "token-b" {
		t.Fatalf("unexpected second target: %+v", targets[1])
	}
	if targets[1].PollInterval != 30*time.Second || targets[1].PollRateLimitPerSec != 2.0 || targets[1].PollBurst != 2 {
		t.Fatalf("unexpected tenant-b poll settings: %+v", targets[1])
	}
	if targets[1].PollFailureBudget != 4 || targets[1].PollCircuitBreak != 20*time.Second || targets[1].PollJitterRatio != 0.2 {
		t.Fatalf("unexpected tenant-b resilience settings: %+v", targets[1])
	}
}

func TestBuildPollTargetsIncludesBaseWhenCredentialsPresent(t *testing.T) {
	base := config.ProviderConfig{
		AccessToken: "base-token",
		Tenants: map[string]config.ProviderTenantConfig{
			"tenant-a": {AccessToken: "token-a"},
		},
	}

	targets := buildPollTargets(base)
	if len(targets) != 2 {
		t.Fatalf("expected base + tenant targets, got %d", len(targets))
	}
	if targets[0].TenantID != "" || targets[0].AccessToken != "base-token" {
		t.Fatalf("unexpected base target: %+v", targets[0])
	}
	if targets[1].TenantID != "tenant-a" || targets[1].AccessToken != "token-a" {
		t.Fatalf("unexpected tenant target: %+v", targets[1])
	}
}

func TestPollLimiterDefaultsAndOverrides(t *testing.T) {
	defaultLimiter, defaultPerSec, defaultBurst := pollLimiter(config.ProviderConfig{})
	if defaultPerSec != 4.0 || defaultBurst != 1 {
		t.Fatalf("unexpected defaults: perSec=%v burst=%d", defaultPerSec, defaultBurst)
	}
	if defaultLimiter.Limit() != 4.0 || defaultLimiter.Burst() != 1 {
		t.Fatalf("unexpected default limiter config: limit=%v burst=%d", defaultLimiter.Limit(), defaultLimiter.Burst())
	}

	customLimiter, customPerSec, customBurst := pollLimiter(config.ProviderConfig{PollRateLimitPerSec: 12.5, PollBurst: 7})
	if customPerSec != 12.5 || customBurst != 7 {
		t.Fatalf("unexpected custom values: perSec=%v burst=%d", customPerSec, customBurst)
	}
	if customLimiter.Limit() != 12.5 || customLimiter.Burst() != 7 {
		t.Fatalf("unexpected custom limiter config: limit=%v burst=%d", customLimiter.Limit(), customLimiter.Burst())
	}
}

func TestPollResilienceDefaultsAndOverrides(t *testing.T) {
	failureBudget, circuitBreak, jitter := pollResilience(config.ProviderConfig{})
	if failureBudget != 5 || circuitBreak != 30*time.Second || jitter != 0 {
		t.Fatalf("unexpected defaults: failureBudget=%d circuitBreak=%s jitter=%v", failureBudget, circuitBreak, jitter)
	}

	failureBudget, circuitBreak, jitter = pollResilience(config.ProviderConfig{
		PollFailureBudget: 9,
		PollCircuitBreak:  50 * time.Second,
		PollJitterRatio:   0.4,
	})
	if failureBudget != 9 || circuitBreak != 50*time.Second || jitter != 0.4 {
		t.Fatalf("unexpected overrides: failureBudget=%d circuitBreak=%s jitter=%v", failureBudget, circuitBreak, jitter)
	}
}
