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

	"github.com/evalops/ensemble-tap/internal/poller"
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
		reqPayload := map[string]any{"limit": limit}
		if !cp.IsZero() {
			reqPayload["filterGroups"] = []any{map[string]any{"filters": []any{map[string]any{
				"propertyName": "hs_lastmodifieddate",
				"operator":     "GT",
				"value":        strconv.FormatInt(cp.UnixMilli(), 10),
			}}}}
		}
		body, _ := json.Marshal(reqPayload)

		url := trimTrailingSlash(h.BaseURL) + "/crm/v3/objects/" + strings.TrimSpace(object) + "/search"
		respBody, err := doAuthenticatedRequest(ctx, client, &token, oauth, func(accessToken string) (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return nil, fmt.Errorf("build hubspot request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+accessToken)
			return req, nil
		})
		if err != nil {
			return poller.FetchResult{}, fmt.Errorf("hubspot request failed: %w", err)
		}

		var out struct {
			Results []struct {
				ID         string         `json:"id"`
				Properties map[string]any `json:"properties"`
			} `json:"results"`
		}
		if err := json.Unmarshal(respBody, &out); err != nil {
			return poller.FetchResult{}, fmt.Errorf("decode hubspot response: %w", err)
		}

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
	}
	h.Token = token

	return poller.FetchResult{Entities: entities, NextCheckpoint: formatCheckpoint(next, checkpoint)}, nil
}
