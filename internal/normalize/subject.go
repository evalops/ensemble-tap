package normalize

import "strings"

func BuildType(provider, entityType, action string) string {
	return strings.ToLower("siphon.tap." + sanitize(provider) + "." + sanitize(entityType) + "." + sanitize(action))
}

func BuildSubject(prefix, provider, entityType, action string) string {
	return strings.ToLower(strings.TrimSuffix(prefix, ".") + "." + sanitize(provider) + "." + sanitize(entityType) + "." + sanitize(action))
}

func BuildSubjectWithTenant(prefix, tenantID, provider, entityType, action string, tenantScoped bool) string {
	base := strings.TrimSuffix(prefix, ".")
	if tenantScoped && strings.TrimSpace(tenantID) != "" {
		base += "." + sanitize(tenantID)
	}
	return strings.ToLower(base + "." + sanitize(provider) + "." + sanitize(entityType) + "." + sanitize(action))
}

func sanitize(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "/", "_", "-", "_", ":", "_", ".", "_")
	return replacer.Replace(s)
}
