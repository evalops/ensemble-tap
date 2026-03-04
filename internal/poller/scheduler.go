package poller

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/cenkalti/backoff/v5"
	"golang.org/x/time/rate"
)

type PollFn func(context.Context) error

type Poller struct {
	Provider             string
	Interval             time.Duration
	RateLimiter          *rate.Limiter
	Backoff              backoff.BackOff
	FailureBudget        int
	CircuitBreakDuration time.Duration
	JitterRatio          float64
	Run                  PollFn
}

func (p *Poller) Start(ctx context.Context) {
	if p == nil || p.Run == nil {
		return
	}
	if p.Interval <= 0 {
		p.Interval = time.Minute
	}
	if p.JitterRatio < 0 {
		p.JitterRatio = 0
	}
	if p.JitterRatio > 0.95 {
		p.JitterRatio = 0.95
	}
	if p.FailureBudget <= 0 {
		p.FailureBudget = 5
	}
	if p.CircuitBreakDuration <= 0 {
		p.CircuitBreakDuration = 30 * time.Second
	}
	bo := p.Backoff
	if bo == nil {
		exp := backoff.NewExponentialBackOff()
		exp.InitialInterval = 500 * time.Millisecond
		exp.MaxInterval = 15 * time.Second
		bo = exp
	}
	bo.Reset()

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	failureCount := 0
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.RateLimiter != nil {
				if err := p.RateLimiter.Wait(ctx); err != nil {
					continue
				}
			}
			if err := p.Run(ctx); err != nil {
				failureCount++
				wait := nextWaitDuration(err, bo)
				wait = applyJitter(wait, p.JitterRatio, rnd)

				if failureCount >= p.FailureBudget {
					if wait < p.CircuitBreakDuration {
						wait = p.CircuitBreakDuration
					}
					failureCount = 0
					bo.Reset()
				}

				if !sleepContext(ctx, wait) {
					return
				}
				continue
			}
			failureCount = 0
			bo.Reset()
		}
	}
}

func nextWaitDuration(err error, bo backoff.BackOff) time.Duration {
	var rl RateLimitedError
	if errors.As(err, &rl) {
		if rl.RetryAfter > 0 {
			return rl.RetryAfter
		}
		return 2 * time.Second
	}
	wait := bo.NextBackOff()
	if wait <= 0 {
		return time.Second
	}
	return wait
}

func applyJitter(wait time.Duration, ratio float64, rnd *rand.Rand) time.Duration {
	if wait <= 0 || ratio <= 0 || rnd == nil {
		return wait
	}
	delta := ratio * (rnd.Float64()*2 - 1) // [-ratio, +ratio]
	jittered := float64(wait) * (1 + delta)
	if jittered < float64(10*time.Millisecond) {
		return 10 * time.Millisecond
	}
	return time.Duration(jittered)
}

func sleepContext(ctx context.Context, wait time.Duration) bool {
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
