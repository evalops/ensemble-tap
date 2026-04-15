package providers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/evalops/siphon/config"
	"github.com/stripe/stripe-go/v82/webhook"
)

func TestStripeHandler(t *testing.T) {
	h := StripeHandler{}
	secret := "whsec_test"
	body := []byte(`{"id":"evt_123","object":"event","api_version":"2025-08-27.basil","type":"invoice.paid","created":1709562120,"data":{"object":{"id":"in_123","object":"invoice"}}}`)
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
		Payload:   body,
		Secret:    secret,
		Timestamp: time.Now().UTC(),
	})

	req := httptest.NewRequest("POST", "/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signed.Header)

	evt, err := h.Handle(req, body, config.ProviderConfig{Secret: secret, TenantID: "tenant-1"})
	if err != nil {
		t.Fatalf("expected valid stripe webhook, got error: %v", err)
	}
	if evt.DedupID != "evt_123" {
		t.Fatalf("unexpected dedup id: %q", evt.DedupID)
	}
	if evt.Normalized.Provider != "stripe" || evt.Normalized.EntityType != "invoice" || evt.Normalized.Action != "paid" {
		t.Fatalf("unexpected normalized event: %+v", evt.Normalized)
	}

	badReq := httptest.NewRequest("POST", "/webhooks/stripe", bytes.NewReader(body))
	badReq.Header.Set("Stripe-Signature", "t=1,v1=deadbeef")
	if _, err := h.Handle(badReq, body, config.ProviderConfig{Secret: secret}); err == nil {
		t.Fatalf("expected invalid signature to fail")
	}
}

func TestGitHubHandler(t *testing.T) {
	h := GitHubHandler{}
	secret := "github-secret"
	body := []byte(`{"action":"closed","pull_request":{"id":99}}`)

	req := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signHex(secret, body, "sha256="))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("Content-Type", "application/json")

	evt, err := h.Handle(req, body, config.ProviderConfig{Secret: secret, TenantID: "tenant-gh"})
	if err != nil {
		t.Fatalf("expected valid github webhook, got error: %v", err)
	}
	if evt.DedupID != "delivery-1" {
		t.Fatalf("unexpected dedup id: %q", evt.DedupID)
	}
	if evt.Normalized.Provider != "github" || evt.Normalized.EntityType != "pull_request" || evt.Normalized.Action != "closed" {
		t.Fatalf("unexpected normalized event: %+v", evt.Normalized)
	}

	missingEventReq := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	missingEventReq.Header.Set("X-Hub-Signature-256", signHex(secret, body, "sha256="))
	missingEventReq.Header.Set("Content-Type", "application/json")
	if _, err := h.Handle(missingEventReq, body, config.ProviderConfig{Secret: secret}); err == nil {
		t.Fatalf("expected missing event header to fail")
	}
}

func TestShopifyHandler(t *testing.T) {
	h := ShopifyHandler{}
	secret := "shopify-secret"
	body := []byte(`{"id":123}`)

	req := httptest.NewRequest("POST", "/webhooks/shopify", bytes.NewReader(body))
	req.Header.Set("X-Shopify-Hmac-SHA256", signBase64(secret, body))
	req.Header.Set("X-Shopify-Topic", "orders/paid")
	req.Header.Set("X-Shopify-Event-Id", "shop_evt_1")

	evt, err := h.Handle(req, body, config.ProviderConfig{Secret: secret, TenantID: "tenant-shop"})
	if err != nil {
		t.Fatalf("expected valid shopify webhook, got error: %v", err)
	}
	if evt.DedupID != "shop_evt_1" {
		t.Fatalf("unexpected dedup id: %q", evt.DedupID)
	}
	if evt.Normalized.EntityType != "orders" || evt.Normalized.Action != "paid" || evt.Normalized.EntityID != "123" {
		t.Fatalf("unexpected normalized event: %+v", evt.Normalized)
	}

	badReq := httptest.NewRequest("POST", "/webhooks/shopify", bytes.NewReader(body))
	badReq.Header.Set("X-Shopify-Hmac-SHA256", "invalid")
	if _, err := h.Handle(badReq, body, config.ProviderConfig{Secret: secret}); err == nil {
		t.Fatalf("expected invalid signature to fail")
	}
}

func TestHubSpotHandler(t *testing.T) {
	h := HubSpotHandler{}
	secret := "hubspot-secret"
	body := []byte(`[{"subscriptionType":"deal.propertyChange","objectType":"deal","objectId":321,"propertyName":"dealstage","propertyValue":"closedwon"}]`)
	cfg := config.ProviderConfig{ClientSecret: secret, TenantID: "tenant-hs"}

	ts := fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	req := httptest.NewRequest("POST", "https://tap.example.com/webhooks/hubspot?foo=bar", bytes.NewReader(body))
	req.Header.Set("X-HubSpot-Request-Timestamp", ts)
	req.Header.Set("X-HubSpot-Event-Id", "hs_evt_1")
	req.Header.Set("X-HubSpot-Signature-v3", signHubSpot(secret, req.Method, resolveRequestURL(req), body, ts))

	evt, err := h.Handle(req, body, cfg)
	if err != nil {
		t.Fatalf("expected valid hubspot webhook, got error: %v", err)
	}
	if evt.DedupID != "hs_evt_1" {
		t.Fatalf("unexpected dedup id: %q", evt.DedupID)
	}
	if evt.Normalized.Provider != "hubspot" || evt.Normalized.EntityType != "deal" || evt.Normalized.EntityID != "321" {
		t.Fatalf("unexpected normalized event: %+v", evt.Normalized)
	}

	oldTS := fmt.Sprintf("%d", time.Now().Add(-10*time.Minute).UTC().UnixMilli())
	oldReq := httptest.NewRequest("POST", "https://tap.example.com/webhooks/hubspot", bytes.NewReader(body))
	oldReq.Header.Set("X-HubSpot-Request-Timestamp", oldTS)
	oldReq.Header.Set("X-HubSpot-Signature-v3", signHubSpot(secret, oldReq.Method, resolveRequestURL(oldReq), body, oldTS))
	if _, err := h.Handle(oldReq, body, cfg); err == nil {
		t.Fatalf("expected expired hubspot timestamp to fail")
	}
}

func TestLinearHandler(t *testing.T) {
	h := LinearHandler{}
	secret := "linear-secret"
	freshBody := []byte(fmt.Sprintf(`{"type":"Issue","data":{"id":"ISS-123"},"webhookTimestamp":"%s"}`,
		time.Now().UTC().Format(time.RFC3339)))

	req := httptest.NewRequest("POST", "/webhooks/linear", bytes.NewReader(freshBody))
	req.Header.Set("Linear-Signature", signHex(secret, freshBody, ""))
	req.Header.Set("Linear-Event", "issue.updated")
	req.Header.Set("Linear-Delivery", "lin_del_1")

	evt, err := h.Handle(req, freshBody, config.ProviderConfig{Secret: secret, TenantID: "tenant-linear"})
	if err != nil {
		t.Fatalf("expected valid linear webhook, got error: %v", err)
	}
	if evt.DedupID != "lin_del_1" {
		t.Fatalf("unexpected dedup id: %q", evt.DedupID)
	}
	if evt.Normalized.EntityType != "issue" || evt.Normalized.Action != "updated" || evt.Normalized.EntityID != "ISS-123" {
		t.Fatalf("unexpected normalized event: %+v", evt.Normalized)
	}

	staleBody := []byte(fmt.Sprintf(`{"type":"Issue","data":{"id":"ISS-123"},"webhookTimestamp":"%s"}`,
		time.Now().Add(-2*time.Minute).UTC().Format(time.RFC3339)))
	staleReq := httptest.NewRequest("POST", "/webhooks/linear", bytes.NewReader(staleBody))
	staleReq.Header.Set("Linear-Signature", signHex(secret, staleBody, ""))
	staleReq.Header.Set("Linear-Event", "issue.updated")
	if _, err := h.Handle(staleReq, staleBody, config.ProviderConfig{Secret: secret}); err == nil {
		t.Fatalf("expected replay-window check to fail")
	}
}

func signHubSpot(secret, method, requestURL string, body []byte, timestamp string) string {
	signed := []byte(method + requestURL + string(body) + timestamp)
	return signBase64(secret, signed)
}

func signBase64(secret string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(payload)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func signHex(secret string, payload []byte, prefix string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(payload)
	return prefix + hex.EncodeToString(h.Sum(nil))
}
