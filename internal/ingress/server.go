package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/siphon/config"
	"github.com/evalops/siphon/internal/dlq"
	"github.com/evalops/siphon/internal/health"
	"github.com/evalops/siphon/internal/ingress/providers"
	"github.com/evalops/siphon/internal/normalize"
)

type Publisher interface {
	Publish(ctx context.Context, event cloudevents.Event, dedupID string) (string, error)
}

type Server struct {
	cfg        config.Config
	publisher  Publisher
	dlq        DLQRecorder
	metrics    *health.Metrics
	logger     *slog.Logger
	handlers   map[string]providers.Handler
	generic    providers.GenericHandler
	httpServer *http.Server
}

type DLQRecorder interface {
	Record(ctx context.Context, rec dlq.Record) error
}

func NewServer(cfg config.Config, publisher Publisher, metrics *health.Metrics, logger *slog.Logger) *Server {
	handlers := map[string]providers.Handler{
		"stripe":  providers.StripeHandler{},
		"github":  providers.GitHubHandler{},
		"shopify": providers.ShopifyHandler{},
		"hubspot": providers.HubSpotHandler{},
		"linear":  providers.LinearHandler{},
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:       cfg,
		publisher: publisher,
		metrics:   metrics,
		logger:    logger,
		handlers:  handlers,
		generic:   providers.GenericHandler{},
	}
}

func (s *Server) SetDLQRecorder(recorder DLQRecorder) {
	s.dlq = recorder
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	base := strings.TrimSpace(s.cfg.Server.BasePath)
	if base == "" {
		base = "/webhooks"
	}
	base = strings.TrimSuffix(base, "/")
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	pattern := "POST " + base + "/{provider}"
	mux.HandleFunc(pattern, s.handleWebhook)
	patternTenant := "POST " + base + "/{provider}/{tenant}"
	mux.HandleFunc(patternTenant, s.handleWebhook)
	return mux
}

func (s *Server) HTTPServer(handler http.Handler) *http.Server {
	if handler == nil {
		handler = s.Routes()
	}
	addr := fmt.Sprintf(":%d", s.cfg.Server.Port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       s.cfg.Server.ReadTimeout,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      s.cfg.Server.WriteTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	s.httpServer = httpServer
	return httpServer
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := webhookRequestID(r)
	w.Header().Set("X-Request-ID", reqID)
	provider := strings.ToLower(strings.TrimSpace(r.PathValue("provider")))
	if provider == "" {
		http.Error(w, "provider missing", http.StatusBadRequest)
		return
	}

	tenantFromPath := strings.TrimSpace(r.PathValue("tenant"))
	tenantFromHeader := strings.TrimSpace(r.Header.Get("X-Tap-Tenant"))
	tenantID := tenantFromPath
	if tenantID == "" {
		tenantID = tenantFromHeader
	}
	cfg, ok := s.resolveProviderConfig(provider, tenantID)
	if !ok {
		http.Error(w, "provider not configured", http.StatusNotFound)
		return
	}

	handler, ok := s.handlers[provider]
	if !ok {
		handler = s.generic
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.Server.MaxBodySize))
	if err != nil {
		s.observe(provider, "rejected", start)
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// Some providers (for example GitHub helpers) read from r.Body during verification.
	// Rewind body after initial size-bounded read.
	r.Body = io.NopCloser(bytes.NewReader(body))

	hooked, err := handler.Handle(r, body, cfg)
	if err != nil {
		if s.metrics != nil {
			s.metrics.WebhookVerificationFailuresTotal.WithLabelValues(provider).Inc()
		}
		s.recordDLQ(r.Context(), dlq.Record{
			Stage:           "verify",
			Provider:        provider,
			TenantID:        cfg.TenantID,
			RequestID:       reqID,
			Reason:          err.Error(),
			OriginalSubject: r.URL.Path,
			OriginalPayload: body,
		})
		s.observe(provider, "rejected", start)
		s.logger.Warn("webhook rejected", "provider", provider, "request_id", reqID, "error", err)
		http.Error(w, "signature verification failed", http.StatusUnauthorized)
		return
	}
	hooked.Normalized.RequestID = reqID

	ce, err := normalize.ToCloudEvent(hooked.Normalized)
	if err != nil {
		s.recordDLQ(r.Context(), dlq.Record{
			Stage:           "normalize",
			Provider:        provider,
			TenantID:        cfg.TenantID,
			RequestID:       reqID,
			Reason:          err.Error(),
			OriginalSubject: r.URL.Path,
			OriginalDedupID: hooked.DedupID,
			OriginalPayload: body,
		})
		s.observe(provider, "error", start)
		s.logger.Error("normalize webhook", "provider", provider, "request_id", reqID, "error", err)
		http.Error(w, "normalization failed", http.StatusInternalServerError)
		return
	}

	dedupID := hooked.DedupID
	if dedupID == "" {
		dedupID = hashDedupID(hooked.Normalized)
	}
	if strings.TrimSpace(cfg.TenantID) != "" {
		dedupID = cfg.TenantID + ":" + dedupID
	}
	subject, err := s.publisher.Publish(r.Context(), ce, dedupID)
	if err != nil {
		payload, _ := json.Marshal(ce)
		s.recordDLQ(r.Context(), dlq.Record{
			Stage:     "publish",
			Provider:  provider,
			TenantID:  cfg.TenantID,
			RequestID: reqID,
			Reason:    err.Error(),
			OriginalSubject: normalize.BuildSubjectWithTenant(
				s.cfg.NATS.SubjectPrefix,
				hooked.Normalized.TenantID,
				hooked.Normalized.Provider,
				hooked.Normalized.EntityType,
				hooked.Normalized.Action,
				s.cfg.NATS.TenantScopedSubjects,
			),
			OriginalDedupID: dedupID,
			OriginalPayload: payload,
		})
		s.observe(provider, "error", start)
		s.logger.Error("publish webhook", "provider", provider, "request_id", reqID, "error", err)
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}

	s.observe(provider, "accepted", start)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":     "accepted",
		"request_id": reqID,
		"id":         ce.ID(),
		"type":       ce.Type(),
		"subject":    subject,
	})
}

func (s *Server) observe(provider, status string, started time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.WebhooksReceivedTotal.WithLabelValues(provider, status).Inc()
	s.metrics.WebhookProcessingDurationSeconds.WithLabelValues(provider).Observe(time.Since(started).Seconds())
}

func hashDedupID(evt normalize.NormalizedEvent) string {
	raw := strings.Join([]string{
		evt.Provider,
		evt.TenantID,
		evt.EntityType,
		evt.EntityID,
		evt.Action,
		evt.ProviderTime.UTC().Format(time.RFC3339Nano),
	}, "|")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (s *Server) recordDLQ(ctx context.Context, rec dlq.Record) {
	if s.dlq == nil {
		return
	}
	_ = s.dlq.Record(ctx, rec)
}

func (s *Server) resolveProviderConfig(provider, tenantID string) (config.ProviderConfig, bool) {
	base, ok := s.cfg.Providers[provider]
	if !ok {
		return config.ProviderConfig{}, false
	}
	if strings.TrimSpace(tenantID) == "" {
		return base, true
	}
	return config.ApplyProviderTenant(base, tenantID), true
}

func webhookRequestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id := strings.TrimSpace(r.Header.Get("X-Request-ID")); id != "" {
		return id
	}
	if id := strings.TrimSpace(r.Header.Get("X-Correlation-ID")); id != "" {
		return id
	}
	return "ing-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}
