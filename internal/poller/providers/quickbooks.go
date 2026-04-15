package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/poller"
)

type QuickBooksFetcher struct {
	HTTPClient   *http.Client
	BaseURL      string
	AccessToken  string
	TokenURL     string
	ClientID     string
	ClientSecret string
	RefreshToken string
	Scope        string
	RealmID      string
	Entities     []string
	QueryPerPage int
	MaxPages     int
	MaxRequests  int
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
	maxPages, maxRequests := normalizeFetchBudget(q.MaxPages, q.MaxRequests)
	requestCount := 0
	pageCount := 0
	truncated := false

	cp := parseCheckpoint(checkpoint)
	next := cp
	entities := make([]poller.Entity, 0)
	client := clientOrDefault(q.HTTPClient)
	base := trimTrailingSlash(q.BaseURL)
	token := strings.TrimSpace(q.AccessToken)
	oauth := OAuthRefreshConfig{
		TokenURL:     q.TokenURL,
		ClientID:     q.ClientID,
		ClientSecret: q.ClientSecret,
		RefreshToken: q.RefreshToken,
		Scope:        q.Scope,
	}

	for _, entityName := range entitiesList {
		if truncated {
			break
		}
		startPosition := 1
		seenEntityNames := map[string]struct{}{}
		for {
			if requestCount >= maxRequests || pageCount >= maxPages {
				truncated = true
				break
			}
			query := fmt.Sprintf("SELECT * FROM %s", entityName)
			if !cp.IsZero() {
				query += " WHERE MetaData.LastUpdatedTime > '" + cp.UTC().Format(time.RFC3339) + "'"
			}
			query += " ORDERBY MetaData.LastUpdatedTime STARTPOSITION " + strconv.Itoa(startPosition) + " MAXRESULTS " + strconv.Itoa(limit)

			endpoint := fmt.Sprintf("%s/v3/company/%s/query?query=%s", base, q.RealmID, url.QueryEscape(query))
			body, err := doAuthenticatedRequest(ctx, client, &token, oauth, "quickbooks", func(accessToken string) (*http.Request, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				if err != nil {
					return nil, fmt.Errorf("build quickbooks request: %w", err)
				}
				req.Header.Set("Authorization", "Bearer "+accessToken)
				req.Header.Set("Accept", "application/json")
				return req, nil
			})
			requestCount++
			if err != nil {
				return poller.FetchResult{}, fmt.Errorf("quickbooks request failed: %w", err)
			}

			var out struct {
				QueryResponse map[string]any `json:"QueryResponse"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				return poller.FetchResult{}, fmt.Errorf("decode quickbooks response: %w", err)
			}
			pageCount++
			qr := out.QueryResponse

			pageRecords := 0
			totalCount := 0
			if v := toString(qr["totalCount"]); v != "" {
				totalCount, _ = strconv.Atoi(v)
			}

			for key, value := range qr {
				if key == "startPosition" || key == "maxResults" || key == "totalCount" {
					continue
				}
				records, ok := value.([]any)
				if !ok {
					continue
				}
				seenEntityNames[key] = struct{}{}
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
					pageRecords++
				}
			}

			// If no records were returned for this page, pagination is complete.
			if pageRecords == 0 {
				break
			}
			// Without totalCount, a short page indicates completion.
			if totalCount == 0 && pageRecords < limit {
				break
			}

			nextPosition := startPosition + pageRecords
			// Stop when server-reported total count has been exhausted.
			if totalCount > 0 && nextPosition > totalCount {
				break
			}
			// Defensive loop break: if the next cursor would not move forward.
			if nextPosition <= startPosition {
				break
			}
			// Stop if entity name mapping is unstable, to avoid mixing collections between pages.
			if len(seenEntityNames) > 1 {
				break
			}
			if pageCount >= maxPages {
				truncated = true
				break
			}
			startPosition = nextPosition
		}
	}
	q.AccessToken = token

	return poller.FetchResult{
		Entities:       entities,
		NextCheckpoint: formatCheckpoint(next, checkpoint),
		Stats: poller.FetchStats{
			Requests:  requestCount,
			Pages:     pageCount,
			Truncated: truncated,
		},
	}, nil
}
