package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/evalops/siphon/config"
	tapv1 "github.com/evalops/siphon/proto/tap/v1"
	"github.com/evalops/siphon/proto/tap/v1/tapv1connect"
)

func TestTapAdminConnectReplayLifecycleAndIdempotency(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_CONNECT_REPLAY",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                 port,
			BasePath:             "/webhooks",
			MaxBodySize:          1 << 20,
			AdminToken:           "test-admin-token",
			AdminRateLimitPerSec: 100,
			AdminRateLimitBurst:  100,
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

	client := newTapAdminConnectClient("http://127.0.0.1:" + intToString(port))

	enqueueReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:          2,
		DryRun:         true,
		IdempotencyKey: "connect-idem-1",
	})
	withAdminHeaders(enqueueReq.Header(), "test-admin-token", "connect-replay-1")
	enqueueResp, err := client.EnqueueReplayJob(ctx, enqueueReq)
	if err != nil {
		t.Fatalf("enqueue replay job: %v", err)
	}
	if enqueueResp.Msg.GetJob().GetJobId() == "" {
		t.Fatalf("expected replay job id in connect response")
	}

	reusedReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:          2,
		DryRun:         true,
		IdempotencyKey: "connect-idem-1",
	})
	withAdminHeaders(reusedReq.Header(), "test-admin-token", "connect-replay-2")
	reusedResp, err := client.EnqueueReplayJob(ctx, reusedReq)
	if err != nil {
		t.Fatalf("reuse replay job: %v", err)
	}
	if !reusedResp.Msg.GetIdempotencyReused() {
		t.Fatalf("expected idempotency_reused=true on replay reuse")
	}

	conflictReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:          3,
		DryRun:         false,
		IdempotencyKey: "connect-idem-1",
	})
	withAdminHeaders(conflictReq.Header(), "test-admin-token", "connect-replay-3")
	_, err = client.EnqueueReplayJob(ctx, conflictReq)
	if got := connect.CodeOf(err); got != connect.CodeAborted {
		t.Fatalf("expected idempotency conflict code %v, got %v err=%v", connect.CodeAborted, got, err)
	}

	jobID := enqueueResp.Msg.GetJob().GetJobId()
	statusResp := waitForReplayJobStatus(t, ctx, client, "test-admin-token", jobID, tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED)
	if got := statusResp.Msg.GetJob().GetStatus(); got != tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED {
		t.Fatalf("expected replay status succeeded, got %v", got)
	}

	listReq := connect.NewRequest(&tapv1.ListReplayJobsRequest{
		Status: tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED,
		Limit:  10,
	})
	withAdminHeaders(listReq.Header(), "test-admin-token", "connect-replay-4")
	listResp, err := client.ListReplayJobs(ctx, listReq)
	if err != nil {
		t.Fatalf("list replay jobs: %v", err)
	}
	found := false
	for _, job := range listResp.Msg.GetJobs() {
		if job.GetJobId() == jobID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected replay job %q in succeeded list", jobID)
	}

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

func TestTapAdminConnectRoleScopedTokens(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_CONNECT_ROLE_SCOPES",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                     port,
			BasePath:                 "/webhooks",
			MaxBodySize:              1 << 20,
			AdminTokenRead:           "read-admin-token",
			AdminTokenReplay:         "replay-admin-token",
			AdminTokenCancel:         "cancel-admin-token",
			AdminReplayRequireReason: true,
			AdminReplayReasonMinLen:  8,
			AdminRateLimitPerSec:     100,
			AdminRateLimitBurst:      100,
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

	client := newTapAdminConnectClient("http://127.0.0.1:" + intToString(port))

	replayReadReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:       1,
		DryRun:      true,
		AdminReason: "validate replay scope",
	})
	withAdminHeaders(replayReadReq.Header(), "read-admin-token", "connect-role-1")
	_, err := client.EnqueueReplayJob(ctx, replayReadReq)
	if got := connect.CodeOf(err); got != connect.CodeUnauthenticated {
		t.Fatalf("expected read token replay to be unauthenticated, got %v err=%v", got, err)
	}

	replayMissingReasonReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:  1,
		DryRun: true,
	})
	withAdminHeaders(replayMissingReasonReq.Header(), "replay-admin-token", "connect-role-2")
	_, err = client.EnqueueReplayJob(ctx, replayMissingReasonReq)
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("expected missing replay reason to be invalid argument, got %v err=%v", got, err)
	}

	replayOKReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:       1,
		DryRun:      true,
		AdminReason: "manually verify connect replay scope",
	})
	withAdminHeaders(replayOKReq.Header(), "replay-admin-token", "connect-role-3")
	replayOKResp, err := client.EnqueueReplayJob(ctx, replayOKReq)
	if err != nil {
		t.Fatalf("enqueue replay with replay token: %v", err)
	}
	if replayOKResp.Msg.GetJob().GetJobId() == "" {
		t.Fatalf("expected replay job id from replay token call")
	}

	pollerReq := connect.NewRequest(&tapv1.GetPollerStatusRequest{})
	withAdminHeaders(pollerReq.Header(), "read-admin-token", "connect-role-4")
	pollerResp, err := client.GetPollerStatus(ctx, pollerReq)
	if err != nil {
		t.Fatalf("poller status with read token: %v", err)
	}
	if pollerResp.Msg.GetCount() != 0 {
		t.Fatalf("expected zero pollers in minimal runtime, got %d", pollerResp.Msg.GetCount())
	}

	cancelMissingReasonReq := connect.NewRequest(&tapv1.CancelReplayJobRequest{
		JobId: "missing-role-job",
	})
	withAdminHeaders(cancelMissingReasonReq.Header(), "cancel-admin-token", "connect-role-5")
	_, err = client.CancelReplayJob(ctx, cancelMissingReasonReq)
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("expected missing cancel reason to be invalid argument, got %v err=%v", got, err)
	}

	cancelReplayTokenReq := connect.NewRequest(&tapv1.CancelReplayJobRequest{
		JobId:       "missing-role-job",
		AdminReason: "should fail",
	})
	withAdminHeaders(cancelReplayTokenReq.Header(), "replay-admin-token", "connect-role-6")
	_, err = client.CancelReplayJob(ctx, cancelReplayTokenReq)
	if got := connect.CodeOf(err); got != connect.CodeUnauthenticated {
		t.Fatalf("expected replay token cancel to be unauthenticated, got %v err=%v", got, err)
	}

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

func TestTapAdminConnectPreservesAllowlistAndMTLS(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "SIPHON_CMD_TEST_CONNECT_MTLS",
			SubjectPrefix: "siphon.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:                      port,
			BasePath:                  "/webhooks",
			MaxBodySize:               1 << 20,
			AdminToken:                "test-admin-token",
			AdminAllowedCIDRs:         []string{"203.0.113.0/24"},
			AdminMTLSRequired:         true,
			AdminMTLSClientCertHeader: "X-Client-Cert",
			AdminRateLimitPerSec:      100,
			AdminRateLimitBurst:       100,
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

	client := newTapAdminConnectClient("http://127.0.0.1:" + intToString(port))

	blockedReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:  1,
		DryRun: true,
	})
	withAdminHeaders(blockedReq.Header(), "test-admin-token", "connect-mtls-1")
	blockedReq.Header().Set("X-Forwarded-For", "198.51.100.20")
	_, err := client.EnqueueReplayJob(ctx, blockedReq)
	if got := connect.CodeOf(err); got != connect.CodePermissionDenied {
		t.Fatalf("expected blocked cidr to be permission denied, got %v err=%v", got, err)
	}

	missingMTLSReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:  1,
		DryRun: true,
	})
	withAdminHeaders(missingMTLSReq.Header(), "test-admin-token", "connect-mtls-2")
	missingMTLSReq.Header().Set("X-Forwarded-For", "203.0.113.22")
	_, err = client.EnqueueReplayJob(ctx, missingMTLSReq)
	if got := connect.CodeOf(err); got != connect.CodePermissionDenied {
		t.Fatalf("expected missing mTLS to be permission denied, got %v err=%v", got, err)
	}

	allowedReq := connect.NewRequest(&tapv1.EnqueueReplayJobRequest{
		Limit:  1,
		DryRun: true,
	})
	withAdminHeaders(allowedReq.Header(), "test-admin-token", "connect-mtls-3")
	allowedReq.Header().Set("X-Forwarded-For", "203.0.113.24")
	allowedReq.Header().Set("X-Client-Cert", "subject=client-a")
	allowedResp, err := client.EnqueueReplayJob(ctx, allowedReq)
	if err != nil {
		t.Fatalf("enqueue replay with allowlist and mTLS: %v", err)
	}
	if allowedResp.Msg.GetJob().GetJobId() == "" {
		t.Fatalf("expected replay job id from allowed request")
	}

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

func TestTapAdminDoAdminJSONPreservesPeerAndOverridesExtraHeaders(t *testing.T) {
	var gotReason string
	var gotIdempotencyKey string
	var gotRemoteAddr string

	server := tapAdminConnectServer{
		adminMux: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotReason = r.Header.Get("X-Admin-Reason")
			gotIdempotencyKey = r.Header.Get("Idempotency-Key")
			gotRemoteAddr = r.RemoteAddr
			w.Header().Set("X-Request-ID", "connect-unit-1")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{}`))
		}),
	}

	sourceHeaders := http.Header{}
	sourceHeaders.Set("X-Admin-Reason", "from-header")
	sourceHeaders.Set("Idempotency-Key", "from-header")
	sourceHeaders.Set("X-Admin-Token", "test-admin-token")

	extraHeaders := http.Header{}
	extraHeaders.Set("X-Admin-Reason", "from-field")
	extraHeaders.Set("Idempotency-Key", "from-field")

	headers, _, status, err := server.doAdminJSON(
		context.Background(),
		connect.Peer{Addr: "198.51.100.10:4567"},
		sourceHeaders,
		http.MethodPost,
		"/admin/replay-dlq",
		nil,
		extraHeaders,
	)
	if err != nil {
		t.Fatalf("doAdminJSON returned error: %v", err)
	}
	if status != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", status)
	}
	if gotReason != "from-field" {
		t.Fatalf("expected extra admin reason to override header, got %q", gotReason)
	}
	if gotIdempotencyKey != "from-field" {
		t.Fatalf("expected extra idempotency key to override header, got %q", gotIdempotencyKey)
	}
	if gotRemoteAddr != "198.51.100.10:4567" {
		t.Fatalf("expected remote addr to be preserved, got %q", gotRemoteAddr)
	}
	if headers.Get("X-Request-ID") != "connect-unit-1" {
		t.Fatalf("expected response request id to propagate, got %q", headers.Get("X-Request-ID"))
	}
}

func newTapAdminConnectClient(baseURL string) tapv1connect.TapAdminServiceClient {
	return tapv1connect.NewTapAdminServiceClient(http.DefaultClient, baseURL)
}

func withAdminHeaders(header http.Header, token, requestID string) {
	header.Set("X-Admin-Token", token)
	header.Set("X-Request-ID", requestID)
}

func waitForReplayJobStatus(t *testing.T, ctx context.Context, client tapv1connect.TapAdminServiceClient, token, jobID string, want tapv1.ReplayJobStatus) *connect.Response[tapv1.GetReplayJobResponse] {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req := connect.NewRequest(&tapv1.GetReplayJobRequest{JobId: jobID})
		withAdminHeaders(req.Header(), token, "connect-status-"+jobID)
		resp, err := client.GetReplayJob(ctx, req)
		if err == nil && resp.Msg.GetJob().GetStatus() == want {
			return resp
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replay job %q to reach status %v", jobID, want)
	return nil
}
