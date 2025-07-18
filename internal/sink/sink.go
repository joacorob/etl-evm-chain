package sink

// Event represents a generic decoded event ready to be persisted.
// Keys are field names and values are their respective data.
// This flexible structure allows different sink back-ends (CSV, MySQL, etc.)
// to decide how to serialize and store the data.
//
// NOTE: More specific strongly-typed representations can be introduced later
// if required by downstream consumers.
//
// The reason to keep it generic for now is to allow the indexer orchestration
// to progress without being blocked by storage details.
type Event map[string]interface{}

// Sink defines the behaviour expected from any storage back-end used by the
// indexer (e.g. CSV files, MySQL, Postgres, webhooks, etc.).
//
// Implementations should be thread-safe if they will be accessed concurrently.
// For now the interface is kept minimal; new capabilities (batch inserts,
// flushing, closing, etc.) can be added as the project evolves.
//
// Returning an error allows the indexer to trigger the retry mechanism
// configured at a higher level.
//
// A no-op implementation can be used for testing.
type Sink interface {
    // Write persists the provided event and returns an error if the operation
    // fails for any reason.
    Write(Event) error
} 