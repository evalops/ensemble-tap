package health

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	WebhooksReceivedTotal            *prometheus.CounterVec
	WebhookVerificationFailuresTotal *prometheus.CounterVec
	WebhookProcessingDurationSeconds *prometheus.HistogramVec
	EventsPublishedTotal             *prometheus.CounterVec
	EventPublishFailuresTotal        *prometheus.CounterVec
	EventsDedupHitsTotal             *prometheus.CounterVec
	AdminRequestsTotal               *prometheus.CounterVec
	AdminRequestDurationSeconds      *prometheus.HistogramVec
	ProviderHealth                   *prometheus.GaugeVec
	NATSConnected                    prometheus.Gauge
}

var (
	metricsOnce sync.Once
	metricsInst *Metrics
)

func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		metricsInst = &Metrics{
			WebhooksReceivedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_webhooks_received_total",
				Help: "Received webhooks by provider and status.",
			}, []string{"provider", "status"}),
			WebhookVerificationFailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_webhooks_verification_failures_total",
				Help: "Webhook signature verification failures by provider.",
			}, []string{"provider"}),
			WebhookProcessingDurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "tap_webhooks_processing_duration_seconds",
				Help:    "Webhook processing latency by provider.",
				Buckets: prometheus.DefBuckets,
			}, []string{"provider"}),
			EventsPublishedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_published_total",
				Help: "Events published to NATS by provider/entity/action.",
			}, []string{"provider", "entity_type", "action"}),
			EventPublishFailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_publish_failures_total",
				Help: "NATS publish failures by provider.",
			}, []string{"provider"}),
			EventsDedupHitsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_dedup_hits_total",
				Help: "Events deduplicated by NATS in the dedup window.",
			}, []string{"provider"}),
			AdminRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_admin_requests_total",
				Help: "Admin endpoint requests by endpoint and outcome.",
			}, []string{"endpoint", "outcome"}),
			AdminRequestDurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "tap_admin_request_duration_seconds",
				Help:    "Admin endpoint request latency by endpoint and outcome.",
				Buckets: prometheus.DefBuckets,
			}, []string{"endpoint", "outcome"}),
			ProviderHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "tap_provider_health",
				Help: "Provider health state (1 healthy, 0 unhealthy).",
			}, []string{"provider"}),
			NATSConnected: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "tap_nats_connected",
				Help: "NATS connection status (1 connected, 0 disconnected).",
			}),
		}
	})
	return metricsInst
}
