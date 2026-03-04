package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/evalops/ensemble-tap/internal/poller"
)

type SalesforceFetcher struct {
	HTTPClient   *http.Client
	BaseURL      string
	AccessToken  string
	APIVersion   string
	Objects      []string
	QueryPerPage int
}

func (s *SalesforceFetcher) ProviderName() string { return "salesforce" }

func (s *SalesforceFetcher) Fetch(ctx context.Context, checkpoint string) (poller.FetchResult, error) {
	if err := require(s.BaseURL, "salesforce base_url"); err != nil {
		return poller.FetchResult{}, err
	}
	if err := require(s.AccessToken, "salesforce access_token"); err != nil {
		return poller.FetchResult{}, err
	}

	apiVersion := strings.TrimSpace(s.APIVersion)
	if apiVersion == "" {
		apiVersion = "v59.0"
	}
	objects := s.Objects
	if len(objects) == 0 {
		objects = []string{"Opportunity"}
	}
	limit := s.QueryPerPage
	if limit <= 0 {
		limit = 200
	}

	cp := parseCheckpoint(checkpoint)
	next := cp
	entities := make([]poller.Entity, 0)
	client := clientOrDefault(s.HTTPClient)
	base := trimTrailingSlash(s.BaseURL)

	for _, object := range objects {
		soql := fmt.Sprintf("SELECT Id, LastModifiedDate FROM %s", object)
		if !cp.IsZero() {
			soql += " WHERE LastModifiedDate > " + cp.UTC().Format("2006-01-02T15:04:05Z")
		}
		soql += fmt.Sprintf(" ORDER BY LastModifiedDate ASC LIMIT %d", limit)

		nextURL := base + "/services/data/" + apiVersion + "/query?q=" + url.QueryEscape(soql)
		for nextURL != "" {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
			if err != nil {
				return poller.FetchResult{}, fmt.Errorf("build salesforce request: %w", err)
			}
			req.Header.Set("Authorization", "Bearer "+s.AccessToken)
			req.Header.Set("Accept", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				return poller.FetchResult{}, fmt.Errorf("salesforce request failed: %w", err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 300 {
				return poller.FetchResult{}, fmt.Errorf("salesforce request failed: status %d", resp.StatusCode)
			}

			var out struct {
				Records        []map[string]any `json:"records"`
				NextRecordsURL string           `json:"nextRecordsUrl"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				return poller.FetchResult{}, fmt.Errorf("decode salesforce response: %w", err)
			}

			for _, rec := range out.Records {
				id := toString(rec["Id"])
				if id == "" {
					continue
				}
				updated := parseTimeAny(rec["LastModifiedDate"])
				if updated.IsZero() {
					updated = time.Now().UTC()
				}
				if next.IsZero() || updated.After(next) {
					next = updated
				}
				entities = append(entities, poller.Entity{
					Provider:   "salesforce",
					EntityType: strings.ToLower(strings.TrimSpace(object)),
					EntityID:   id,
					Snapshot:   cloneMap(rec),
					UpdatedAt:  updated,
				})
			}

			nextURL = ""
			if strings.TrimSpace(out.NextRecordsURL) != "" {
				if strings.HasPrefix(out.NextRecordsURL, "http") {
					nextURL = out.NextRecordsURL
				} else {
					nextURL = base + out.NextRecordsURL
				}
			}
		}
	}

	return poller.FetchResult{Entities: entities, NextCheckpoint: formatCheckpoint(next, checkpoint)}, nil
}
