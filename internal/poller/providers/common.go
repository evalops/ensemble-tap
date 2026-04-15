package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/poller"
)

const (
	defaultFetchMaxPages    = 200
	defaultFetchMaxRequests = 400
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

type OAuthRefreshConfig struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	RefreshToken string
	Scope        string
}

func (c OAuthRefreshConfig) enabled() bool {
	return strings.TrimSpace(c.TokenURL) != "" && strings.TrimSpace(c.ClientID) != "" && strings.TrimSpace(c.ClientSecret) != ""
}

func doAuthenticatedRequest(
	ctx context.Context,
	client *http.Client,
	token *string,
	oauth OAuthRefreshConfig,
	provider string,
	buildReq func(accessToken string) (*http.Request, error),
) ([]byte, error) {
	perform := func(accessToken string) (*http.Response, []byte, error) {
		req, err := buildReq(accessToken)
		if err != nil {
			return nil, nil, err
		}
		if err := validateOutboundRequest(req); err != nil {
			return nil, nil, err
		}
		// #nosec G704 -- outbound URL is validated and sourced from operator-controlled provider config.
		resp, err := client.Do(req)
		if err != nil {
			return nil, nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp, body, nil
	}

	accessToken := strings.TrimSpace(*token)
	resp, body, err := perform(accessToken)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		if oauth.enabled() {
			refreshed, refreshErr := refreshAccessToken(ctx, client, oauth)
			if refreshErr != nil {
				return nil, fmt.Errorf("token refresh failed: %w", refreshErr)
			}
			*token = refreshed
			resp, body, err = perform(refreshed)
			if err != nil {
				return nil, err
			}
		}
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		if strings.TrimSpace(provider) == "" {
			provider = "poller"
		}
		return nil, poller.RateLimitedError{
			Provider:   provider,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), 2*time.Second),
		}
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, truncate(string(body), 256))
	}
	return body, nil
}

func refreshAccessToken(ctx context.Context, client *http.Client, cfg OAuthRefreshConfig) (string, error) {
	tokenURL, err := url.Parse(strings.TrimSpace(cfg.TokenURL))
	if err != nil {
		return "", fmt.Errorf("parse token url: %w", err)
	}
	if err := validateOutboundURL(tokenURL); err != nil {
		return "", fmt.Errorf("invalid token url: %w", err)
	}

	values := url.Values{}
	values.Set("client_id", cfg.ClientID)
	values.Set("client_secret", cfg.ClientSecret)
	if strings.TrimSpace(cfg.RefreshToken) != "" {
		values.Set("grant_type", "refresh_token")
		values.Set("refresh_token", cfg.RefreshToken)
	} else {
		values.Set("grant_type", "client_credentials")
	}
	if strings.TrimSpace(cfg.Scope) != "" {
		values.Set("scope", cfg.Scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL.String(), bytes.NewBufferString(values.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// #nosec G704 -- token endpoint URL is validated and sourced from operator-controlled provider config.
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("token endpoint status %d: %s", resp.StatusCode, truncate(string(body), 256))
	}

	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.AccessToken) == "" {
		return "", fmt.Errorf("token endpoint did not return access_token")
	}
	return out.AccessToken, nil
}

func parseRetryAfter(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if when, err := time.Parse(time.RFC1123, raw); err == nil {
		d := time.Until(when)
		if d > 0 {
			return d
		}
	}
	return fallback
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func normalizeFetchBudget(maxPages, maxRequests int) (int, int) {
	if maxPages <= 0 {
		maxPages = defaultFetchMaxPages
	}
	if maxRequests <= 0 {
		maxRequests = defaultFetchMaxRequests
	}
	return maxPages, maxRequests
}

func validateOutboundRequest(req *http.Request) error {
	if req == nil || req.URL == nil {
		return fmt.Errorf("request URL is required")
	}
	return validateOutboundURL(req.URL)
}

func validateOutboundURL(raw *url.URL) error {
	if raw == nil {
		return fmt.Errorf("URL is required")
	}
	scheme := strings.ToLower(strings.TrimSpace(raw.Scheme))
	if scheme != "https" && scheme != "http" {
		return fmt.Errorf("unsupported URL scheme %q", raw.Scheme)
	}
	if strings.TrimSpace(raw.Hostname()) == "" {
		return fmt.Errorf("missing URL host")
	}
	if raw.User != nil {
		return fmt.Errorf("URL userinfo is not allowed")
	}
	return nil
}
