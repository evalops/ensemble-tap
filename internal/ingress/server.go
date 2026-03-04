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
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/ingress/providers"
	"github.com/evalops/ensemble-tap/internal/normalize"
)

type Publisher interface {
	Publish(ctx context.Context, event cloudevents.Event, dedupID string) (string, error)
}

type Server struct {
	cfg        config.Config
	publisher  Publisher
	metrics    *health.Metrics
	logger     *slog.Logger
	handlers   map[string]providers.Handler
	generic    providers.GenericHandler
	httpServer *http.Server
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
	return mux
}

func (s *Server) HTTPServer(handler http.Handler) *http.Server {
	if handler == nil {
		handler = s.Routes()
	}
	addr := fmt.Sprintf(":%d", s.cfg.Server.Port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  s.cfg.Server.ReadTimeout,
		WriteTimeout: s.cfg.Server.WriteTimeout,
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
	provider := strings.ToLower(strings.TrimSpace(r.PathValue("provider")))
	if provider == "" {
		http.Error(w, "provider missing", http.StatusBadRequest)
		return
	}

	cfg, ok := s.cfg.Providers[provider]
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
		s.observe(provider, "rejected", start)
		s.logger.Warn("webhook rejected", "provider", provider, "error", err)
		http.Error(w, "signature verification failed", http.StatusUnauthorized)
		return
	}

	ce, err := normalize.ToCloudEvent(hooked.Normalized)
	if err != nil {
		s.observe(provider, "error", start)
		s.logger.Error("normalize webhook", "provider", provider, "error", err)
		http.Error(w, "normalization failed", http.StatusInternalServerError)
		return
	}

	dedupID := hooked.DedupID
	if dedupID == "" {
		dedupID = hashDedupID(hooked.Normalized)
	}
	subject, err := s.publisher.Publish(r.Context(), ce, dedupID)
	if err != nil {
		s.observe(provider, "error", start)
		s.logger.Error("publish webhook", "provider", provider, "error", err)
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}

	s.observe(provider, "accepted", start)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"id":      ce.ID(),
		"type":    ce.Type(),
		"subject": subject,
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
		evt.EntityType,
		evt.EntityID,
		evt.Action,
		evt.ProviderTime.UTC().Format(time.RFC3339Nano),
	}, "|")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
