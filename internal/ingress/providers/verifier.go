package providers

import (
	"crypto/hmac"
	"crypto/sha1" // #nosec G505 -- compatibility for providers that still sign with SHA-1.
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ProviderVerifier struct {
	Algorithm          string
	SigHeader          string
	SigEncoding        string
	SigPrefix          string
	TimestampHeader    string
	MaxAge             time.Duration
	BuildSignedPayload func(r *http.Request, body []byte) ([]byte, error)
}

func (v ProviderVerifier) Verify(r *http.Request, body []byte, secret string) error {
	if secret == "" {
		return fmt.Errorf("missing secret")
	}

	sig := strings.TrimSpace(r.Header.Get(v.SigHeader))
	if sig == "" {
		return fmt.Errorf("missing signature header %q", v.SigHeader)
	}

	if v.TimestampHeader != "" && v.MaxAge > 0 {
		ts, err := parseTimestamp(r.Header.Get(v.TimestampHeader))
		if err != nil {
			return fmt.Errorf("invalid timestamp header %q: %w", v.TimestampHeader, err)
		}
		age := time.Since(ts)
		if age < 0 {
			age = -age
		}
		if age > v.MaxAge {
			return fmt.Errorf("request timestamp outside max age window")
		}
	}

	signed := body
	if v.BuildSignedPayload != nil {
		var err error
		signed, err = v.BuildSignedPayload(r, body)
		if err != nil {
			return fmt.Errorf("build signed payload: %w", err)
		}
	}

	expected, err := computeMAC(v.Algorithm, []byte(secret), signed)
	if err != nil {
		return err
	}

	provided, err := decodeSignature(sig, v.SigEncoding, v.SigPrefix)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	if !hmac.Equal(expected, provided) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func computeMAC(algorithm string, secret, payload []byte) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(algorithm)) {
	case "sha1":
		h := hmac.New(sha1.New, secret)
		_, _ = h.Write(payload)
		return h.Sum(nil), nil
	case "sha256", "":
		h := hmac.New(sha256.New, secret)
		_, _ = h.Write(payload)
		return h.Sum(nil), nil
	default:
		return nil, fmt.Errorf("unsupported algorithm %q", algorithm)
	}
}

func decodeSignature(raw, encoding, prefix string) ([]byte, error) {
	value := strings.TrimSpace(raw)
	if prefix != "" {
		value = strings.TrimPrefix(value, prefix)
	}
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "hex", "prefixed-hex", "":
		b, err := hex.DecodeString(value)
		if err != nil {
			return nil, err
		}
		return b, nil
	case "base64", "prefixed-base64":
		b, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported signature encoding %q", encoding)
	}
}

func parseTimestamp(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		return ts, nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	if sec, err := parseInt64(raw); err == nil {
		if len(raw) >= 13 {
			return time.UnixMilli(sec), nil
		}
		return time.Unix(sec, 0), nil
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format")
}

func parseInt64(v string) (int64, error) {
	var n int64
	for _, r := range v {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}
