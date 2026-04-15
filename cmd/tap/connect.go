package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	tapv1 "github.com/evalops/siphon/proto/tap/v1"
	"github.com/evalops/siphon/proto/tap/v1/tapv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type tapAdminConnectServer struct {
	adminMux http.Handler
}

type replayJobEnvelopeJSON struct {
	RequestID         string                 `json:"request_id"`
	IdempotencyReused bool                   `json:"idempotency_reused,omitempty"`
	Job               adminReplayJobSnapshot `json:"job"`
}

type listReplayJobsResponseJSON struct {
	RequestID  string                   `json:"request_id"`
	Status     string                   `json:"status"`
	Limit      int                      `json:"limit"`
	Cursor     string                   `json:"cursor"`
	NextCursor string                   `json:"next_cursor"`
	Count      int                      `json:"count"`
	Summary    map[string]int           `json:"summary"`
	Jobs       []adminReplayJobSnapshot `json:"jobs"`
}

type pollerStatusResponseJSON struct {
	GeneratedAt time.Time              `json:"generated_at"`
	RequestID   string                 `json:"request_id"`
	Provider    string                 `json:"provider"`
	Tenant      string                 `json:"tenant"`
	Count       int                    `json:"count"`
	Pollers     []pollerStatusSnapshot `json:"pollers"`
}

type adminErrorJSON struct {
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
}

var _ tapv1connect.TapAdminServiceHandler = (*tapAdminConnectServer)(nil)

func newTapAdminConnectHandler(adminMux http.Handler) (string, http.Handler) {
	return tapv1connect.NewTapAdminServiceHandler(&tapAdminConnectServer{adminMux: adminMux})
}

func (s *tapAdminConnectServer) ListReplayJobs(ctx context.Context, req *connect.Request[tapv1.ListReplayJobsRequest]) (*connect.Response[tapv1.ListReplayJobsResponse], error) {
	query := url.Values{}
	if status := replayJobStatusToString(req.Msg.GetStatus()); status != "" {
		query.Set("status", status)
	}
	if limit := req.Msg.GetLimit(); limit != 0 {
		query.Set("limit", strconv.Itoa(int(limit)))
	}
	if cursor := strings.TrimSpace(req.Msg.GetCursor()); cursor != "" {
		query.Set("cursor", cursor)
	}

	headers, body, status, err := s.doAdminJSON(ctx, req.Peer(), req.Header(), http.MethodGet, "/admin/replay-dlq", query, nil)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		return nil, connectErrorFromAdminHTTP(status, headers, body)
	}

	var payload listReplayJobsResponseJSON
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode replay job list response: %w", err))
	}
	resp := connect.NewResponse(&tapv1.ListReplayJobsResponse{
		RequestId:  payload.RequestID,
		Status:     replayJobStatusFromString(payload.Status),
		Limit:      safeInt32(payload.Limit),
		Cursor:     payload.Cursor,
		NextCursor: payload.NextCursor,
		Count:      safeInt32(payload.Count),
		Summary:    replayJobSummaryToProto(payload.Summary),
		Jobs:       replayJobsToProto(payload.Jobs),
	})
	copyAdminResponseHeaders(resp.Header(), headers)
	return resp, nil
}

func (s *tapAdminConnectServer) EnqueueReplayJob(ctx context.Context, req *connect.Request[tapv1.EnqueueReplayJobRequest]) (*connect.Response[tapv1.EnqueueReplayJobResponse], error) {
	query := url.Values{}
	if limit := req.Msg.GetLimit(); limit != 0 {
		query.Set("limit", strconv.Itoa(int(limit)))
	}
	if req.Msg.GetDryRun() {
		query.Set("dry_run", "true")
	}
	extraHeaders := http.Header{}
	if idempotencyKey := strings.TrimSpace(req.Msg.GetIdempotencyKey()); idempotencyKey != "" {
		extraHeaders.Set("Idempotency-Key", idempotencyKey)
	}
	if adminReason := strings.TrimSpace(req.Msg.GetAdminReason()); adminReason != "" {
		extraHeaders.Set("X-Admin-Reason", adminReason)
	}

	headers, body, status, err := s.doAdminJSON(ctx, req.Peer(), req.Header(), http.MethodPost, "/admin/replay-dlq", query, extraHeaders)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		return nil, connectErrorFromAdminHTTP(status, headers, body)
	}

	return decodeEnqueueReplayJobResponse(headers, body)
}

func (s *tapAdminConnectServer) GetReplayJob(ctx context.Context, req *connect.Request[tapv1.GetReplayJobRequest]) (*connect.Response[tapv1.GetReplayJobResponse], error) {
	jobID := strings.TrimSpace(req.Msg.GetJobId())
	if jobID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("job_id is required"))
	}

	headers, body, status, err := s.doAdminJSON(ctx, req.Peer(), req.Header(), http.MethodGet, "/admin/replay-dlq/"+url.PathEscape(jobID), nil, nil)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		return nil, connectErrorFromAdminHTTP(status, headers, body)
	}

	return decodeGetReplayJobResponse(headers, body)
}

func (s *tapAdminConnectServer) CancelReplayJob(ctx context.Context, req *connect.Request[tapv1.CancelReplayJobRequest]) (*connect.Response[tapv1.CancelReplayJobResponse], error) {
	jobID := strings.TrimSpace(req.Msg.GetJobId())
	if jobID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("job_id is required"))
	}
	extraHeaders := http.Header{}
	if adminReason := strings.TrimSpace(req.Msg.GetAdminReason()); adminReason != "" {
		extraHeaders.Set("X-Admin-Reason", adminReason)
	}

	headers, body, status, err := s.doAdminJSON(ctx, req.Peer(), req.Header(), http.MethodDelete, "/admin/replay-dlq/"+url.PathEscape(jobID), nil, extraHeaders)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		return nil, connectErrorFromAdminHTTP(status, headers, body)
	}

	return decodeCancelReplayJobResponse(headers, body)
}

func (s *tapAdminConnectServer) GetPollerStatus(ctx context.Context, req *connect.Request[tapv1.GetPollerStatusRequest]) (*connect.Response[tapv1.GetPollerStatusResponse], error) {
	query := url.Values{}
	if provider := strings.TrimSpace(req.Msg.GetProvider()); provider != "" {
		query.Set("provider", provider)
	}
	if tenant := strings.TrimSpace(req.Msg.GetTenant()); tenant != "" {
		query.Set("tenant", tenant)
	}

	headers, body, status, err := s.doAdminJSON(ctx, req.Peer(), req.Header(), http.MethodGet, "/admin/poller-status", query, nil)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		return nil, connectErrorFromAdminHTTP(status, headers, body)
	}

	var payload pollerStatusResponseJSON
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode poller status response: %w", err))
	}
	resp := connect.NewResponse(&tapv1.GetPollerStatusResponse{
		GeneratedAt: timestampOrNil(payload.GeneratedAt),
		RequestId:   payload.RequestID,
		Provider:    payload.Provider,
		Tenant:      payload.Tenant,
		Count:       safeInt32(payload.Count),
		Pollers:     pollerStatusesToProto(payload.Pollers),
	})
	copyAdminResponseHeaders(resp.Header(), headers)
	return resp, nil
}

func (s *tapAdminConnectServer) doAdminJSON(ctx context.Context, peer connect.Peer, sourceHeaders http.Header, method, path string, query url.Values, extraHeaders http.Header) (http.Header, []byte, int, error) {
	target := &url.URL{Path: path}
	if len(query) > 0 {
		target.RawQuery = query.Encode()
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, nil, 0, connect.NewError(connect.CodeInternal, fmt.Errorf("build admin request: %w", err))
	}
	copyHTTPHeaders(httpReq.Header, sourceHeaders)
	setHTTPHeaders(httpReq.Header, extraHeaders)
	httpReq.RemoteAddr = peer.Addr

	recorder := httptest.NewRecorder()
	s.adminMux.ServeHTTP(recorder, httpReq)

	resp := recorder.Result()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, nil, 0, connect.NewError(connect.CodeInternal, fmt.Errorf("read admin response: %w", err))
	}
	return resp.Header, body, resp.StatusCode, nil
}

func decodeEnqueueReplayJobResponse(headers http.Header, body []byte) (*connect.Response[tapv1.EnqueueReplayJobResponse], error) {
	var payload replayJobEnvelopeJSON
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode replay job response: %w", err))
	}
	resp := connect.NewResponse(&tapv1.EnqueueReplayJobResponse{
		RequestId:         payload.RequestID,
		IdempotencyReused: payload.IdempotencyReused,
		Job:               replayJobToProto(payload.Job),
	})
	copyAdminResponseHeaders(resp.Header(), headers)
	return resp, nil
}

func decodeGetReplayJobResponse(headers http.Header, body []byte) (*connect.Response[tapv1.GetReplayJobResponse], error) {
	var payload replayJobEnvelopeJSON
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode replay job response: %w", err))
	}
	resp := connect.NewResponse(&tapv1.GetReplayJobResponse{
		RequestId: payload.RequestID,
		Job:       replayJobToProto(payload.Job),
	})
	copyAdminResponseHeaders(resp.Header(), headers)
	return resp, nil
}

func decodeCancelReplayJobResponse(headers http.Header, body []byte) (*connect.Response[tapv1.CancelReplayJobResponse], error) {
	var payload replayJobEnvelopeJSON
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode replay job response: %w", err))
	}
	resp := connect.NewResponse(&tapv1.CancelReplayJobResponse{
		RequestId: payload.RequestID,
		Job:       replayJobToProto(payload.Job),
	})
	copyAdminResponseHeaders(resp.Header(), headers)
	return resp, nil
}

func copyHTTPHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func setHTTPHeaders(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyAdminResponseHeaders(dst, src http.Header) {
	if requestID := strings.TrimSpace(src.Get("X-Request-ID")); requestID != "" {
		dst.Set("X-Request-ID", requestID)
	}
}

func connectErrorFromAdminHTTP(status int, headers http.Header, body []byte) error {
	code := connect.CodeInternal
	switch status {
	case http.StatusBadRequest:
		code = connect.CodeInvalidArgument
	case http.StatusUnauthorized:
		code = connect.CodeUnauthenticated
	case http.StatusForbidden:
		code = connect.CodePermissionDenied
	case http.StatusNotFound:
		code = connect.CodeNotFound
	case http.StatusConflict:
		code = connect.CodeAborted
	case http.StatusTooManyRequests:
		code = connect.CodeResourceExhausted
	}

	message := http.StatusText(status)
	var payload adminErrorJSON
	if err := json.Unmarshal(body, &payload); err == nil {
		if strings.TrimSpace(payload.Error) != "" {
			message = payload.Error
		}
	}
	connectErr := connect.NewError(code, errors.New(message))
	if requestID := strings.TrimSpace(headers.Get("X-Request-ID")); requestID != "" {
		connectErr.Meta().Set("X-Request-ID", requestID)
	} else if payload.RequestID != "" {
		connectErr.Meta().Set("X-Request-ID", payload.RequestID)
	}
	if retryAfter := strings.TrimSpace(headers.Get("Retry-After")); retryAfter != "" {
		connectErr.Meta().Set("Retry-After", retryAfter)
	}
	return connectErr
}

func replayJobsToProto(jobs []adminReplayJobSnapshot) []*tapv1.ReplayJob {
	out := make([]*tapv1.ReplayJob, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, replayJobToProto(job))
	}
	return out
}

func replayJobToProto(job adminReplayJobSnapshot) *tapv1.ReplayJob {
	return &tapv1.ReplayJob{
		JobId:          job.JobID,
		Status:         replayJobStatusFromString(job.Status),
		RequestedLimit: safeInt32(job.RequestedLimit),
		EffectiveLimit: safeInt32(job.EffectiveLimit),
		MaxLimit:       safeInt32(job.MaxLimit),
		Capped:         job.Capped,
		DryRun:         job.DryRun,
		CreatedAt:      timestampOrNil(job.CreatedAt),
		StartedAt:      timestampOrNil(job.StartedAt),
		CompletedAt:    timestampOrNil(job.CompletedAt),
		Replayed:       safeInt32(job.Replayed),
		RequestId:      job.RequestID,
		OperatorReason: job.OperatorReason,
		CancelReason:   job.CancelReason,
		Error:          job.Error,
	}
}

func replayJobSummaryToProto(summary map[string]int) []*tapv1.ReplayJobStatusCount {
	if len(summary) == 0 {
		return nil
	}
	knownOrder := []tapv1.ReplayJobStatus{
		tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_QUEUED,
		tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_RUNNING,
		tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED,
		tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_FAILED,
		tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_CANCELLED,
	}
	seen := make(map[string]struct{}, len(knownOrder))
	out := make([]*tapv1.ReplayJobStatusCount, 0, len(summary))
	for _, status := range knownOrder {
		key := replayJobStatusToString(status)
		seen[key] = struct{}{}
		count, ok := summary[key]
		if !ok {
			continue
		}
		out = append(out, &tapv1.ReplayJobStatusCount{Status: status, Count: safeInt32(count)})
	}
	var unknown []string
	for key := range summary {
		if _, ok := seen[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	for _, key := range unknown {
		out = append(out, &tapv1.ReplayJobStatusCount{
			Status: tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_UNSPECIFIED,
			Count:  safeInt32(summary[key]),
		})
	}
	return out
}

func pollerStatusesToProto(statuses []pollerStatusSnapshot) []*tapv1.PollerStatus {
	out := make([]*tapv1.PollerStatus, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, &tapv1.PollerStatus{
			Provider:             status.Provider,
			TenantId:             status.TenantID,
			Interval:             status.Interval,
			RateLimitPerSec:      status.RateLimitPerSec,
			Burst:                safeInt32(status.Burst),
			FailureBudget:        safeInt32(status.FailureBudget),
			CircuitBreakDuration: status.CircuitBreak,
			JitterRatio:          status.JitterRatio,
			LastRunAt:            timestampOrNil(status.LastRunAt),
			LastSuccessAt:        timestampOrNil(status.LastSuccessAt),
			LastErrorAt:          timestampOrNil(status.LastErrorAt),
			LastError:            status.LastError,
			LastCheckpoint:       status.LastCheckpoint,
			ConsecutiveFailures:  safeInt32(status.ConsecutiveFailures),
		})
	}
	return out
}

func safeInt32(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value.UTC())
}

func replayJobStatusFromString(raw string) tapv1.ReplayJobStatus {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case adminReplayJobStatusQueued:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_QUEUED
	case adminReplayJobStatusRunning:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_RUNNING
	case adminReplayJobStatusSucceeded:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED
	case adminReplayJobStatusFailed:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_FAILED
	case adminReplayJobStatusCancelled:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_CANCELLED
	default:
		return tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_UNSPECIFIED
	}
}

func replayJobStatusToString(status tapv1.ReplayJobStatus) string {
	switch status {
	case tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_QUEUED:
		return adminReplayJobStatusQueued
	case tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_RUNNING:
		return adminReplayJobStatusRunning
	case tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_SUCCEEDED:
		return adminReplayJobStatusSucceeded
	case tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_FAILED:
		return adminReplayJobStatusFailed
	case tapv1.ReplayJobStatus_REPLAY_JOB_STATUS_CANCELLED:
		return adminReplayJobStatusCancelled
	default:
		return ""
	}
}
