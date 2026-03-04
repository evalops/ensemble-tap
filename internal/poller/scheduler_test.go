package poller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type fakeBackoff struct {
	next time.Duration
}

func (f *fakeBackoff) NextBackOff() time.Duration { return f.next }
func (f *fakeBackoff) Reset()                     {}

func TestNextWaitDurationRateLimited(t *testing.T) {
	wait := nextWaitDuration(RateLimitedError{Provider: "hubspot", RetryAfter: 3 * time.Second}, &fakeBackoff{next: time.Second})
	if wait != 3*time.Second {
		t.Fatalf("expected rate-limit wait, got %s", wait)
	}
}

func TestNextWaitDurationFallsBackToBackoff(t *testing.T) {
	wait := nextWaitDuration(errors.New("boom"), &fakeBackoff{next: 2 * time.Second})
	if wait != 2*time.Second {
		t.Fatalf("expected backoff wait, got %s", wait)
	}
}

func TestApplyJitterBoundaries(t *testing.T) {
	wait := 2 * time.Second
	if got := applyJitter(wait, 0, nil); got != wait {
		t.Fatalf("expected unchanged wait without jitter, got %s", got)
	}
}

func TestSleepContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok := sleepContext(ctx, time.Second); ok {
		t.Fatalf("expected sleepContext to stop on canceled context")
	}
}

func TestPollerHandlesRateLimitedError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	p := &Poller{
		Interval:      1 * time.Millisecond,
		JitterRatio:   0,
		FailureBudget: 10,
		Backoff:       &backoff.ZeroBackOff{},
		Run: func(ctx context.Context) error {
			count++
			if count == 1 {
				return RateLimitedError{Provider: "notion", RetryAfter: 120 * time.Millisecond}
			}
			cancel()
			return nil
		},
	}

	start := time.Now()
	p.Start(ctx)
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected poller to respect retry-after, elapsed %s", elapsed)
	}
}
