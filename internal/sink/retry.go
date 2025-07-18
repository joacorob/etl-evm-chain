package sink

import (
	"time"

	"github.com/sirupsen/logrus"
)

// RetrySink decorates another Sink adding automatic retry capabilities.
// It attempts to write the event up to the configured number of attempts,
// waiting the specified delay between retries. This allows the indexer to
// tolerate transient failures in the underlying storage backend without
// needing to add retry logic in multiple places.
//
// If attempts is < 1, it defaults to 1 (no retries).
// If delayMs is 0, it defaults to 1000ms.
//
// The RetrySink propagates the error from the last attempt if all retries
// fail.
type RetrySink struct {
    inner    Sink
    attempts int
    delay    time.Duration
}

// NewRetrySink builds a new Sink with retry behaviour around the provided
// inner sink. The returned value still fulfils the Sink interface so it can
// be used transparently by the rest of the application.
func NewRetrySink(inner Sink, attempts int, delayMs int) Sink {
    if inner == nil {
        return nil
    }
    if attempts < 1 {
        attempts = 1
    }
    if delayMs == 0 {
        delayMs = 1000
    }
    return &RetrySink{
        inner:    inner,
        attempts: attempts,
        delay:    time.Duration(delayMs) * time.Millisecond,
    }
}

// Write forwards the call to the wrapped sink retrying on failure.
func (r *RetrySink) Write(evt Event) error {
    var err error
    for attempt := 1; attempt <= r.attempts; attempt++ {
        err = r.inner.Write(evt)
        if err == nil {
            return nil
        }

        logrus.Warnf("sink write failed (attempt %d/%d): %v", attempt, r.attempts, err)

        // Wait before next retry unless it's the final attempt.
        if attempt < r.attempts {
            time.Sleep(r.delay)
        }
    }
    return err
} 