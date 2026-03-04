package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHubSpotFetcher(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody map[string]any

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{map[string]any{
				"id": "deal_1",
				"properties": map[string]any{
					"hs_lastmodifieddate": "2026-03-03T14:22:00Z",
					"dealname":            "Acme",
				},
			}},
		})
	}))
	defer ts.Close()

	f := &HubSpotFetcher{
		BaseURL: ts.URL,
		Token:   "hubspot-token",
		Objects: []string{"deals"},
	}
	res, err := f.Fetch(context.Background(), "2026-03-03T14:00:00Z")
	if err != nil {
		t.Fatalf("fetch hubspot: %v", err)
	}
	if gotAuth != "Bearer hubspot-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotPath != "/crm/v3/objects/deals/search" {
		t.Fatalf("unexpected path: %q", gotPath)
	}
	if len(res.Entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(res.Entities))
	}
	if res.Entities[0].EntityType != "deals" || res.Entities[0].EntityID != "deal_1" {
		t.Fatalf("unexpected hubspot entity: %+v", res.Entities[0])
	}
	if res.NextCheckpoint != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected hubspot checkpoint: %q", res.NextCheckpoint)
	}
	if _, ok := gotBody["filterGroups"]; !ok {
		t.Fatalf("expected filter groups in hubspot request")
	}
}

func TestSalesforceFetcher(t *testing.T) {
	var gotAuth string
	var gotQuery string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotQuery = r.URL.Query().Get("q")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"records": []any{map[string]any{
				"Id":               "sf_1",
				"LastModifiedDate": "2026-03-03T15:00:00Z",
				"Name":             "Opportunity 1",
			}},
		})
	}))
	defer ts.Close()

	f := &SalesforceFetcher{
		BaseURL:     ts.URL,
		AccessToken: "sf-token",
		Objects:     []string{"Opportunity"},
	}
	res, err := f.Fetch(context.Background(), "2026-03-03T14:00:00Z")
	if err != nil {
		t.Fatalf("fetch salesforce: %v", err)
	}
	if gotAuth != "Bearer sf-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if !strings.Contains(gotQuery, "FROM Opportunity") || !strings.Contains(gotQuery, "LastModifiedDate >") {
		t.Fatalf("unexpected salesforce query: %q", gotQuery)
	}
	if len(res.Entities) != 1 || res.Entities[0].EntityID != "sf_1" {
		t.Fatalf("unexpected salesforce entities: %+v", res.Entities)
	}
}

func TestQuickBooksFetcher(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotQuery string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotQuery = r.URL.Query().Get("query")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"QueryResponse": map[string]any{
				"Customer": []any{map[string]any{
					"Id": "qb_1",
					"MetaData": map[string]any{
						"LastUpdatedTime": "2026-03-03T16:00:00Z",
					},
				}},
			},
		})
	}))
	defer ts.Close()

	f := &QuickBooksFetcher{
		BaseURL:     ts.URL,
		AccessToken: "qb-token",
		RealmID:     "realm-1",
		Entities:    []string{"Customer"},
	}
	res, err := f.Fetch(context.Background(), "2026-03-03T14:00:00Z")
	if err != nil {
		t.Fatalf("fetch quickbooks: %v", err)
	}
	if gotAuth != "Bearer qb-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotPath != "/v3/company/realm-1/query" {
		t.Fatalf("unexpected path: %q", gotPath)
	}
	if !strings.Contains(gotQuery, "MetaData.LastUpdatedTime") {
		t.Fatalf("unexpected quickbooks query: %q", gotQuery)
	}
	if len(res.Entities) != 1 || res.Entities[0].EntityID != "qb_1" {
		t.Fatalf("unexpected quickbooks entities: %+v", res.Entities)
	}
}

func TestNotionFetcherPagination(t *testing.T) {
	var reqBodies []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		reqBodies = append(reqBodies, parsed)

		if len(reqBodies) == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results":     []any{map[string]any{"id": "not_1", "object": "page", "last_edited_time": "2026-03-03T17:00:00Z"}},
				"has_more":    true,
				"next_cursor": "cursor_2",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":     []any{map[string]any{"id": "not_2", "object": "database", "last_edited_time": "2026-03-03T17:30:00Z"}},
			"has_more":    false,
			"next_cursor": "",
		})
	}))
	defer ts.Close()

	f := &NotionFetcher{BaseURL: ts.URL, Token: "notion-token"}
	res, err := f.Fetch(context.Background(), "2026-03-03T16:00:00Z")
	if err != nil {
		t.Fatalf("fetch notion: %v", err)
	}
	if len(res.Entities) != 2 {
		t.Fatalf("expected 2 notion entities, got %d", len(res.Entities))
	}
	if res.NextCheckpoint != "2026-03-03T17:30:00Z" {
		t.Fatalf("unexpected notion checkpoint: %q", res.NextCheckpoint)
	}
	if len(reqBodies) != 2 {
		t.Fatalf("expected 2 notion requests, got %d", len(reqBodies))
	}
	if _, ok := reqBodies[0]["filter"]; !ok {
		t.Fatalf("expected filter in first notion request")
	}
	if reqBodies[1]["start_cursor"] != "cursor_2" {
		t.Fatalf("expected next cursor in second request, got %#v", reqBodies[1]["start_cursor"])
	}
}

func TestCommonParseTimeAndCheckpoint(t *testing.T) {
	if got := parseCheckpoint("2026-03-03T14:22:00Z"); got.Format(time.RFC3339) != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected checkpoint parse: %s", got)
	}
	if got := parseTimeAny("1709562120000"); got.IsZero() {
		t.Fatalf("expected unix ms parse")
	}
	if got := formatCheckpoint(time.Time{}, "fallback"); got != "fallback" {
		t.Fatalf("expected fallback checkpoint, got %q", got)
	}
}

func TestCommonToString(t *testing.T) {
	if toString(float64(12)) != "12" {
		t.Fatalf("float toString failed")
	}
	if toString(42) != "42" || toString(int64(99)) != "99" {
		t.Fatalf("int toString failed")
	}
}

func TestQuickBooksQueryEscapes(t *testing.T) {
	raw := "SELECT * FROM Customer WHERE MetaData.LastUpdatedTime > '2026-03-03T14:00:00Z'"
	encoded := url.QueryEscape(raw)
	if strings.Contains(encoded, " ") {
		t.Fatalf("query should be escaped")
	}
	decoded, err := url.QueryUnescape(encoded)
	if err != nil || decoded != raw {
		t.Fatalf("query escape roundtrip failed")
	}
}
