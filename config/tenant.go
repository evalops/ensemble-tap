package config

import "strings"

func ApplyProviderTenant(base ProviderConfig, tenantKey string) ProviderConfig {
	out := base
	tenantKey = strings.TrimSpace(tenantKey)
	if tenantKey == "" {
		return out
	}
	out.TenantID = tenantKey

	if base.Tenants == nil {
		out.Tenants = nil
		return out
	}
	tenantCfg, ok := base.Tenants[tenantKey]
	if !ok {
		out.Tenants = nil
		return out
	}

	if v := strings.TrimSpace(tenantCfg.TenantID); v != "" {
		out.TenantID = v
	}
	if v := strings.TrimSpace(tenantCfg.Secret); v != "" {
		out.Secret = v
	}
	if v := strings.TrimSpace(tenantCfg.ClientSecret); v != "" {
		out.ClientSecret = v
	}
	if v := strings.TrimSpace(tenantCfg.AccessToken); v != "" {
		out.AccessToken = v
	}
	if v := strings.TrimSpace(tenantCfg.APIKey); v != "" {
		out.APIKey = v
	}
	if v := strings.TrimSpace(tenantCfg.BaseURL); v != "" {
		out.BaseURL = v
	}
	if v := strings.TrimSpace(tenantCfg.RealmID); v != "" {
		out.RealmID = v
	}
	if v := strings.TrimSpace(tenantCfg.RefreshToken); v != "" {
		out.RefreshToken = v
	}
	if tenantCfg.PollInterval > 0 {
		out.PollInterval = tenantCfg.PollInterval
	}
	if tenantCfg.PollRateLimitPerSec > 0 {
		out.PollRateLimitPerSec = tenantCfg.PollRateLimitPerSec
	}
	if tenantCfg.PollBurst > 0 {
		out.PollBurst = tenantCfg.PollBurst
	}
	out.Tenants = nil
	return out
}
