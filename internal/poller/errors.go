package poller

import (
	"fmt"
	"time"
)

type RateLimitedError struct {
	Provider   string
	RetryAfter time.Duration
}

func (e RateLimitedError) Error() string {
	if e.Provider == "" {
		return fmt.Sprintf("rate limited; retry after %s", e.RetryAfter)
	}
	return fmt.Sprintf("%s rate limited; retry after %s", e.Provider, e.RetryAfter)
}
