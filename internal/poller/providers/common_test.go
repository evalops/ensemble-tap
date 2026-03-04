package providers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/evalops/ensemble-tap/internal/poller"
)

func TestDoAuthenticatedRequestRefreshesToken(t *testing.T) {
	requestAttempts := 0
	refreshAttempts := 0
	var mu sync.Mutex
	var handlerErr error

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/data":
			requestAttempts++
			if r.Header.Get("Authorization") != "Bearer fresh-token" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"expired"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/token":
			refreshAttempts++
			if err := r.ParseForm(); err != nil {
				mu.Lock()
				handlerErr = fmt.Errorf("parse form: %w", err)
				mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Form.Get("grant_type") != "refresh_token" {
				mu.Lock()
				handlerErr = fmt.Errorf("unexpected grant_type: %s", r.Form.Get("grant_type"))
				mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"fresh-token"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	token := "stale-token"
	oauth := OAuthRefreshConfig{
		TokenURL:     srv.URL + "/token",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		RefreshToken: "refresh-token",
	}
	body, err := doAuthenticatedRequest(context.Background(), srv.Client(), &token, oauth, func(accessToken string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/data", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		return req, nil
	})
	if err != nil {
		t.Fatalf("doAuthenticatedRequest returned error: %v", err)
	}
	mu.Lock()
	gotHandlerErr := handlerErr
	mu.Unlock()
	if gotHandlerErr != nil {
		t.Fatal(gotHandlerErr)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", string(body))
	}
	if token != "fresh-token" {
		t.Fatalf("expected token refresh, got %q", token)
	}
	if requestAttempts != 2 {
		t.Fatalf("expected two request attempts, got %d", requestAttempts)
	}
	if refreshAttempts != 1 {
		t.Fatalf("expected one refresh attempt, got %d", refreshAttempts)
	}
}

func TestDoAuthenticatedRequestReturnsRateLimitedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	token := "token"
	_, err := doAuthenticatedRequest(context.Background(), srv.Client(), &token, OAuthRefreshConfig{}, func(accessToken string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		return req, nil
	})
	if err == nil {
		t.Fatalf("expected error for rate limited response")
	}

	var rl poller.RateLimitedError
	if !errors.As(err, &rl) {
		t.Fatalf("expected rate limited error, got %T", err)
	}
	if rl.RetryAfter != 3*time.Second {
		t.Fatalf("expected retry-after of 3s, got %s", rl.RetryAfter)
	}
}
