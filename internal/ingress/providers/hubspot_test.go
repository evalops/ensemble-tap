package providers

import (
	"net/http/httptest"
	"testing"
)

func TestResolveRequestURLUsesForwardedHeaders(t *testing.T) {
	r := httptest.NewRequest("POST", "http://internal.local/webhooks/hubspot?foo=bar", nil)
	r.Header.Set("X-Forwarded-Proto", "http")
	r.Header.Set("X-Forwarded-Host", "hooks.example.com")

	got := resolveRequestURL(r)
	want := "http://hooks.example.com/webhooks/hubspot?foo=bar"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestResolveRequestURLDefaultsToHTTPSAndRequestHost(t *testing.T) {
	r := httptest.NewRequest("POST", "http://platform.local/webhooks/hubspot", nil)

	got := resolveRequestURL(r)
	want := "https://platform.local/webhooks/hubspot"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
