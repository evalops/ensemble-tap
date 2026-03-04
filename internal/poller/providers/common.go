package providers

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func clientOrDefault(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return &http.Client{Timeout: 20 * time.Second}
}

func trimTrailingSlash(v string) string {
	return strings.TrimSuffix(strings.TrimSpace(v), "/")
}

func toString(v any) string {
	switch vv := v.(type) {
	case string:
		return strings.TrimSpace(vv)
	case float64:
		return strconv.FormatInt(int64(vv), 10)
	case int:
		return strconv.Itoa(vv)
	case int64:
		return strconv.FormatInt(vv, 10)
	default:
		return ""
	}
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func parseCheckpoint(checkpoint string) time.Time {
	checkpoint = strings.TrimSpace(checkpoint)
	if checkpoint == "" {
		return time.Time{}
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000-0700"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, checkpoint); err == nil {
			return t.UTC()
		}
	}
	if ms, err := strconv.ParseInt(checkpoint, 10, 64); err == nil {
		if len(checkpoint) >= 13 {
			return time.UnixMilli(ms).UTC()
		}
		return time.Unix(ms, 0).UTC()
	}
	return time.Time{}
}

func formatCheckpoint(t time.Time, fallback string) string {
	if t.IsZero() {
		return fallback
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTimeAny(v any) time.Time {
	s := toString(v)
	if s == "" {
		return time.Time{}
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000-0700", "2006-01-02T15:04:05-0700"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if len(s) >= 13 {
			return time.UnixMilli(i).UTC()
		}
		return time.Unix(i, 0).UTC()
	}
	return time.Time{}
}

func require(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}
