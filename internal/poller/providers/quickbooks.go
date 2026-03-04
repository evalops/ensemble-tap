package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/evalops/ensemble-tap/internal/poller"
)

type QuickBooksFetcher struct {
	HTTPClient   *http.Client
	BaseURL      string
	AccessToken  string
	RealmID      string
	Entities     []string
	QueryPerPage int
}

func (q *QuickBooksFetcher) ProviderName() string { return "quickbooks" }

func (q *QuickBooksFetcher) Fetch(ctx context.Context, checkpoint string) (poller.FetchResult, error) {
	if err := require(q.BaseURL, "quickbooks base_url"); err != nil {
		return poller.FetchResult{}, err
	}
	if err := require(q.AccessToken, "quickbooks access_token"); err != nil {
		return poller.FetchResult{}, err
	}
	if err := require(q.RealmID, "quickbooks realm_id"); err != nil {
		return poller.FetchResult{}, err
	}

	entitiesList := q.Entities
	if len(entitiesList) == 0 {
		entitiesList = []string{"Customer"}
	}
	limit := q.QueryPerPage
	if limit <= 0 {
		limit = 100
	}

	cp := parseCheckpoint(checkpoint)
	next := cp
	entities := make([]poller.Entity, 0)
	client := clientOrDefault(q.HTTPClient)
	base := trimTrailingSlash(q.BaseURL)

	for _, entityName := range entitiesList {
		query := fmt.Sprintf("SELECT * FROM %s", entityName)
		if !cp.IsZero() {
			query += " WHERE MetaData.LastUpdatedTime > '" + cp.UTC().Format(time.RFC3339) + "'"
		}
		query += " ORDERBY MetaData.LastUpdatedTime STARTPOSITION 1 MAXRESULTS " + strconv.Itoa(limit)

		endpoint := fmt.Sprintf("%s/v3/company/%s/query?query=%s", base, q.RealmID, url.QueryEscape(query))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return poller.FetchResult{}, fmt.Errorf("build quickbooks request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+q.AccessToken)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return poller.FetchResult{}, fmt.Errorf("quickbooks request failed: %w", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			return poller.FetchResult{}, fmt.Errorf("quickbooks request failed: status %d", resp.StatusCode)
		}

		var out map[string]any
		if err := json.Unmarshal(body, &out); err != nil {
			return poller.FetchResult{}, fmt.Errorf("decode quickbooks response: %w", err)
		}
		qr, _ := out["QueryResponse"].(map[string]any)
		for key, value := range qr {
			if key == "startPosition" || key == "maxResults" || key == "totalCount" {
				continue
			}
			records, ok := value.([]any)
			if !ok {
				continue
			}
			for _, item := range records {
				rec, ok := item.(map[string]any)
				if !ok {
					continue
				}
				id := toString(rec["Id"])
				if id == "" {
					continue
				}
				updated := time.Now().UTC()
				if md, ok := rec["MetaData"].(map[string]any); ok {
					if ts := parseTimeAny(md["LastUpdatedTime"]); !ts.IsZero() {
						updated = ts
					}
				}
				if next.IsZero() || updated.After(next) {
					next = updated
				}
				entities = append(entities, poller.Entity{
					Provider:   "quickbooks",
					EntityType: strings.ToLower(strings.TrimSpace(key)),
					EntityID:   id,
					Snapshot:   cloneMap(rec),
					UpdatedAt:  updated,
				})
			}
		}
	}

	return poller.FetchResult{Entities: entities, NextCheckpoint: formatCheckpoint(next, checkpoint)}, nil
}
