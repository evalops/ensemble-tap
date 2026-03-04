package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/evalops/ensemble-tap/internal/poller"
)

type NotionFetcher struct {
	HTTPClient *http.Client
	BaseURL    string
	Token      string
	PageSize   int
}

func (n *NotionFetcher) ProviderName() string { return "notion" }

func (n *NotionFetcher) Fetch(ctx context.Context, checkpoint string) (poller.FetchResult, error) {
	if err := require(n.BaseURL, "notion base_url"); err != nil {
		return poller.FetchResult{}, err
	}
	if err := require(n.Token, "notion token"); err != nil {
		return poller.FetchResult{}, err
	}

	pageSize := n.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	cp := parseCheckpoint(checkpoint)
	next := cp
	entities := make([]poller.Entity, 0)
	client := clientOrDefault(n.HTTPClient)
	endpoint := trimTrailingSlash(n.BaseURL) + "/v1/search"

	cursor := ""
	for {
		reqBody := map[string]any{
			"page_size": pageSize,
			"sort": map[string]any{
				"direction": "ascending",
				"timestamp": "last_edited_time",
			},
		}
		if cursor != "" {
			reqBody["start_cursor"] = cursor
		}
		if !cp.IsZero() {
			reqBody["filter"] = map[string]any{
				"property":         "timestamp",
				"timestamp":        "last_edited_time",
				"last_edited_time": map[string]any{"after": cp.UTC().Format(time.RFC3339)},
			}
		}
		body, _ := json.Marshal(reqBody)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return poller.FetchResult{}, fmt.Errorf("build notion request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+n.Token)
		req.Header.Set("Notion-Version", "2022-06-28")
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return poller.FetchResult{}, fmt.Errorf("notion request failed: %w", err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			return poller.FetchResult{}, fmt.Errorf("notion request failed: status %d", resp.StatusCode)
		}

		var out struct {
			Results    []map[string]any `json:"results"`
			HasMore    bool             `json:"has_more"`
			NextCursor string           `json:"next_cursor"`
		}
		if err := json.Unmarshal(respBody, &out); err != nil {
			return poller.FetchResult{}, fmt.Errorf("decode notion response: %w", err)
		}

		for _, item := range out.Results {
			id := toString(item["id"])
			if id == "" {
				continue
			}
			entityType := strings.ToLower(strings.TrimSpace(toString(item["object"])))
			if entityType == "" {
				entityType = "page"
			}
			updated := parseTimeAny(item["last_edited_time"])
			if updated.IsZero() {
				updated = time.Now().UTC()
			}
			if next.IsZero() || updated.After(next) {
				next = updated
			}
			entities = append(entities, poller.Entity{
				Provider:   "notion",
				EntityType: entityType,
				EntityID:   id,
				Snapshot:   cloneMap(item),
				UpdatedAt:  updated,
			})
		}

		if !out.HasMore || strings.TrimSpace(out.NextCursor) == "" {
			break
		}
		cursor = out.NextCursor
	}

	return poller.FetchResult{Entities: entities, NextCheckpoint: formatCheckpoint(next, checkpoint)}, nil
}
