package sink

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// csvFile wraps an opened CSV file with its writer and cached headers.
// All writes must respect the header order to keep column consistency.
type csvFile struct {
    file    *os.File
    writer  *csv.Writer
    headers []string
}

// CSVSink persists decoded Ethereum events into per-event CSV files.
// It creates one file per unique event name in the configured output
// directory. The first time an event is seen the sink writes a header row
// containing ALL keys present in the provided Event map (sorted
// alphabetically for determinism) and appends every subsequent row in the
// same column order.
//
// Concurrency note: the Indexer currently calls Sink.Write sequentially, but
// a mutex is included for future-proofing.
type CSVSink struct {
    outputDir string
    mu        sync.Mutex
    files     map[string]*csvFile // keyed by "<contractName>_<eventName>"
}

// NewCSVSink initialises a sink that writes CSV files under the given
// directory, creating the directory tree if it doesn’t already exist.
func NewCSVSink(outputDir string) (*CSVSink, error) {
    if err := os.MkdirAll(outputDir, 0o755); err != nil {
        return nil, fmt.Errorf("failed to create csv output directory: %w", err)
    }

    return &CSVSink{
        outputDir: outputDir,
        files:     make(map[string]*csvFile),
    }, nil
}

// Write appends the provided event as a CSV row. It lazily creates the file
// associated with the event_name (or “unknown” when missing).
func (s *CSVSink) Write(evt Event) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Defensive access to event_name so that even malformed events are stored.
    name, _ := evt["event_name"].(string)
    if name == "" {
        name = "unknown"
    }

    contractName, _ := evt["contract_name"].(string)
    if contractName == "" {
        contractName = "unknown"
    }

    key := contractName + "_" + name

    cf, ok := s.files[key]
    if !ok {
        // First time we see this event – prepare CSV file.
        fp := filepath.Join(s.outputDir, fmt.Sprintf("%s.csv", key))

        // Determine whether file already exists (from a previous run).
        _, err := os.Stat(fp)
        exists := !os.IsNotExist(err)

        // Open file for append & read (read needed when file pre-exists to fetch headers).
        f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
        if err != nil {
            return fmt.Errorf("failed to open csv file %s: %w", fp, err)
        }

        w := csv.NewWriter(f)

        headers := extractHeaders(evt)

        if !exists {
            // New file – write header row immediately.
            if err := w.Write(headers); err != nil {
                f.Close()
                return fmt.Errorf("failed to write csv header for %s: %w", fp, err)
            }
            w.Flush()
            if err := w.Error(); err != nil {
                f.Close()
                return fmt.Errorf("failed to flush csv header for %s: %w", fp, err)
            }
        }

        cf = &csvFile{file: f, writer: w, headers: headers}
        s.files[key] = cf
    }

    // Prepare row following stored header order.
    row := make([]string, len(cf.headers))
    for i, key := range cf.headers {
        if v, ok := evt[key]; ok {
            row[i] = fmt.Sprint(v)
        } else {
            row[i] = ""
        }
    }

    if err := cf.writer.Write(row); err != nil {
        return err
    }
    cf.writer.Flush()
    return cf.writer.Error()
}

// extractHeaders returns a deterministic, alphabetically-sorted slice of map
// keys which will be used as CSV columns.
func extractHeaders(evt Event) []string {
    headers := make([]string, 0, len(evt))
    for k := range evt {
        headers = append(headers, k)
    }
    sort.Strings(headers)
    return headers
} 