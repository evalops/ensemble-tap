package main

import (
	"path/filepath"
	"testing"

	"github.com/evalops/ensemble-tap/config"
	pollproviders "github.com/evalops/ensemble-tap/internal/poller/providers"
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
	cp.Set("hubspot", "cp")
	if got, ok := cp.Get("hubspot"); !ok || got != "cp" {
		t.Fatalf("unexpected memory checkpoint: %q %v", got, ok)
	}
	snap.Put("hubspot", "deal", "1", map[string]any{"stage": "open"})

	sqlitePath := filepath.Join(t.TempDir(), "state.db")
	cp2, _, closer2, err := openPollStores(config.StateConfig{Backend: "sqlite", SQLitePath: sqlitePath})
	if err != nil {
		t.Fatalf("open sqlite stores: %v", err)
	}
	defer closer2.Close()
	cp2.Set("hubspot", "cp2")
	if got, ok := cp2.Get("hubspot"); !ok || got != "cp2" {
		t.Fatalf("unexpected sqlite checkpoint: %q %v", got, ok)
	}

	if _, _, _, err := openPollStores(config.StateConfig{Backend: "unknown"}); err == nil {
		t.Fatalf("expected unsupported backend error")
	}
}
