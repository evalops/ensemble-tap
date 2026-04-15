package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/evalops/siphon/internal/poller"
)

type HubSpotFetcher struct {
	HTTPClient   *http.Client
	BaseURL      string
	Token        string
	TokenURL     string
	ClientID     string
	ClientSecret string
	RefreshToken string
	Scope        string
	Objects      []string
	Limit        int
	MaxPages     int
	MaxRequests  int
}

func (h *HubSpotFetcher) ProviderName() string { return "hubspot" }

func (h *HubSpotFetcher) Fetch(ctx context.Context, checkpoint string) (poller.FetchResult, error) {
	if err := require(h.BaseURL, "hubspot base_url"); err != nil {
		return poller.FetchResult{}, err
	}
	if err := require(h.Token, "hubspot token"); err != nil {
		return poller.FetchResult{}, err
	}

	objects := h.Objects
	if len(objects) == 0 {
		objects = []string{"deals", "contacts", "companies", "tickets"}
	}
	limit := h.Limit
	if limit <= 0 {
		limit = 100
	}
	maxPages, maxRequests := normalizeFetchBudget(h.MaxPages, h.MaxRequests)
	requestCount := 0
	pageCount := 0
	truncated := false

	cp := parseCheckpoint(checkpoint)
	next := cp
	entities := make([]poller.Entity, 0)
	client := clientOrDefault(h.HTTPClient)
	token := strings.TrimSpace(h.Token)
	oauth := OAuthRefreshConfig{
		TokenURL:     h.TokenURL,
		ClientID:     h.ClientID,
		ClientSecret: h.ClientSecret,
		RefreshToken: h.RefreshToken,
		Scope:        h.Scope,
	}

	for _, object := range objects {
		if truncated {
			break
		}
		url := trimTrailingSlash(h.BaseURL) + "/crm/v3/objects/" + strings.TrimSpace(object) + "/search"
		after := ""
		for {
			if requestCount >= maxRequests || pageCount >= maxPages {
				truncated = true
				break
			}
			reqPayload := map[string]any{"limit": limit}
			if !cp.IsZero() {
				reqPayload["filterGroups"] = []any{map[string]any{"filters": []any{map[string]any{
					"propertyName": "hs_lastmodifieddate",
					"operator":     "GT",
					"value":        strconv.FormatInt(cp.UnixMilli(), 10),
				}}}}
			}
			if strings.TrimSpace(after) != "" {
				reqPayload["after"] = after
			}
			body, err := json.Marshal(reqPayload)
			if err != nil {
				return poller.FetchResult{}, fmt.Errorf("marshal hubspot request payload: %w", err)
			}

			respBody, err := doAuthenticatedRequest(ctx, client, &token, oauth, "hubspot", func(accessToken string) (*http.Request, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
				if err != nil {
					return nil, fmt.Errorf("build hubspot request: %w", err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+accessToken)
				return req, nil
			})
			requestCount++
			if err != nil {
				return poller.FetchResult{}, fmt.Errorf("hubspot request failed: %w", err)
			}

			var out struct {
				Results []struct {
					ID         string         `json:"id"`
					Properties map[string]any `json:"properties"`
				} `json:"results"`
				Paging struct {
					Next struct {
						After string `json:"after"`
					} `json:"next"`
				} `json:"paging"`
			}
			if err := json.Unmarshal(respBody, &out); err != nil {
				return poller.FetchResult{}, fmt.Errorf("decode hubspot response: %w", err)
			}
			pageCount++

			for _, item := range out.Results {
				snapshot := cloneMap(item.Properties)
				snapshot["id"] = item.ID
				updated := parseTimeAny(item.Properties["hs_lastmodifieddate"])
				if updated.IsZero() {
					updated = time.Now().UTC()
				}
				if next.IsZero() || updated.After(next) {
					next = updated
				}
				entities = append(entities, poller.Entity{
					Provider:   "hubspot",
					EntityType: strings.ToLower(strings.TrimSpace(object)),
					EntityID:   item.ID,
					Snapshot:   snapshot,
					UpdatedAt:  updated,
				})
			}

			nextAfter := strings.TrimSpace(out.Paging.Next.After)
			if nextAfter == "" || nextAfter == after {
				break
			}
			if pageCount >= maxPages {
				truncated = true
				break
			}
			after = nextAfter
		}
	}
	h.Token = token

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
