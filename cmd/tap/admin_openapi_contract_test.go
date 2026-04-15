package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
)

func TestAdminOpenAPIContractMatchesRuntime(t *testing.T) {
	specPath := filepath.Join("..", "..", "docs", "admin-openapi.yaml")
	loader := &openapi3.Loader{IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load openapi spec: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("validate openapi spec: %v", err)
	}
	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		t.Fatalf("build openapi router: %v", err)
	}

	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_OPENAPI",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:        port,
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
			AdminToken:  "test-admin-token",
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	validateRoundTrip := func(req *http.Request, expectedStatus int) ([]byte, *http.Response) {
		t.Helper()
		specReq := req.Clone(context.Background())
		specReq.URL.Scheme = "http"
		specReq.URL.Host = "localhost:8080"
		route, pathParams, err := router.FindRoute(specReq)
		if err != nil {
			t.Fatalf("find openapi route: %v", err)
		}
		reqValidation := &openapi3filter.RequestValidationInput{
			Request:    specReq,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				AuthenticationFunc: func(context.Context, *openapi3filter.AuthenticationInput) error {
					return nil
				},
			},
		}
		if err := openapi3filter.ValidateRequest(context.Background(), reqValidation); err != nil {
			t.Fatalf("openapi request validation failed: %v", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("send request: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read response body: %v", err)
		}
		if resp.StatusCode != expectedStatus {
			t.Fatalf("expected status %d, got %d body=%s", expectedStatus, resp.StatusCode, string(body))
		}
		responseValidation := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: reqValidation,
			Status:                 resp.StatusCode,
			Header:                 resp.Header,
			Body:                   io.NopCloser(bytes.NewReader(body)),
		}
		if err := openapi3filter.ValidateResponse(context.Background(), responseValidation); err != nil {
			t.Fatalf("openapi response validation failed: %v body=%s", err, string(body))
		}
		return body, resp
	}

	replayReq, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?dry_run=true&limit=2", nil)
	replayReq.Header.Set("X-Admin-Token", "test-admin-token")
	replayReq.Header.Set("X-Request-ID", "openapi-replay-1")
	replayReq.Header.Set("Idempotency-Key", "openapi-idem-1")
	replayBody, _ := validateRoundTrip(replayReq, http.StatusAccepted)

	var replayResp struct {
		Job struct {
			JobID string `json:"job_id"`
		} `json:"job"`
	}
	if err := json.Unmarshal(replayBody, &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResp.Job.JobID == "" {
		t.Fatalf("expected replay response to include job_id")
	}

	replayConflictReq, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?dry_run=false&limit=3", nil)
	replayConflictReq.Header.Set("X-Admin-Token", "test-admin-token")
	replayConflictReq.Header.Set("X-Request-ID", "openapi-replay-conflict-1")
	replayConflictReq.Header.Set("Idempotency-Key", "openapi-idem-1")
	_, _ = validateRoundTrip(replayConflictReq, http.StatusConflict)

	replayListReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?status=succeeded&limit=1", nil)
	replayListReq.Header.Set("X-Admin-Token", "test-admin-token")
	replayListReq.Header.Set("X-Request-ID", "openapi-replay-list-1")
	replayListBody, _ := validateRoundTrip(replayListReq, http.StatusOK)
	var replayListResp struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(replayListBody, &replayListResp); err != nil {
		t.Fatalf("decode replay list response: %v", err)
	}
	if replayListResp.NextCursor != "" {
		replayListNextReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq?status=succeeded&limit=1&cursor="+replayListResp.NextCursor, nil)
		replayListNextReq.Header.Set("X-Admin-Token", "test-admin-token")
		replayListNextReq.Header.Set("X-Request-ID", "openapi-replay-list-2")
		_, _ = validateRoundTrip(replayListNextReq, http.StatusOK)
	}

	statusReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq/"+replayResp.Job.JobID, nil)
	statusReq.Header.Set("X-Admin-Token", "test-admin-token")
	statusReq.Header.Set("X-Request-ID", "openapi-status-1")
	_, _ = validateRoundTrip(statusReq, http.StatusOK)

	cancelReq, _ := http.NewRequest(http.MethodDelete, "http://127.0.0.1:"+intToString(port)+"/admin/replay-dlq/missing-job-id", nil)
	cancelReq.Header.Set("X-Admin-Token", "test-admin-token")
	cancelReq.Header.Set("X-Request-ID", "openapi-cancel-1")
	_, _ = validateRoundTrip(cancelReq, http.StatusNotFound)

	pollerReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+intToString(port)+"/admin/poller-status?provider=notion", nil)
	pollerReq.Header.Set("X-Admin-Token", "test-admin-token")
	pollerReq.Header.Set("X-Request-ID", "openapi-poller-1")
	_, _ = validateRoundTrip(pollerReq, http.StatusOK)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}
