package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"etl-web3/internal/config"
	"etl-web3/internal/indexer"
	"etl-web3/internal/rpc"
	"etl-web3/internal/sink"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/sirupsen/logrus"
)

// handleJobs acts as a multiplexer: POST creates new job, other verbs not allowed.
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createJob(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleJobByID routes GET and DELETE for specific job IDs.
func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	// Expected path: /jobs/{id}
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		http.Error(w, "job id missing", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getJob(w, r, id)
	case http.MethodDelete:
		s.cancelJob(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// createJob handles POST /jobs
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req JobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.RPCURL == "" {
		http.Error(w, "rpc_url is required", http.StatusBadRequest)
		return
	}
	if len(req.Contracts) == 0 {
		http.Error(w, "at least one contract must be provided", http.StatusBadRequest)
		return
	}

	jobID := newUUID()

	status := &JobStatus{
		JobID:     jobID,
		Status:    "queued",
		StartedAt: time.Now(),
	}

	s.mu.Lock()
	s.jobs[jobID] = &jobEntry{status: status}
	s.mu.Unlock()

	go s.runJob(jobID, req)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(JobResponse{JobID: jobID})
}

// runJob converts the request into a Config, initialises dependencies and runs the indexer.
func (s *Server) runJob(jobID string, req JobRequest) {
	// Get job entry to update status later.
	s.mu.Lock()
	entry := s.jobs[jobID]
	// Guard against nil (should not happen)
	if entry == nil {
		entry = &jobEntry{status: &JobStatus{JobID: jobID}}
		s.jobs[jobID] = entry
	}
	// Update status to running
	entry.status.Status = "running"
	s.mu.Unlock()

	// Build config from request
	cfg, err := buildConfigFromRequest(req)
	if err != nil {
		s.markJobError(jobID, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	entry.cancel = cancel

	// Initialise RPC client
	client, err := rpc.Dial(ctx, cfg.RPCURL, cfg.Retry)
	if err != nil {
		s.markJobError(jobID, err)
		return
	}

	// Initialise sink
	var sk sink.Sink
	switch cfg.Storage.Type {
	case "csv":
		sk, err = sink.NewCSVSink(cfg.Storage.CSV.OutputDir)
		if err != nil {
			s.markJobError(jobID, err)
			return
		}
	case "mysql":
		s.markJobError(jobID, fmt.Errorf("mysql sink not implemented"))
		return
	default:
		s.markJobError(jobID, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type))
		return
	}

	// Wrap sink with retry logic
	sk = sink.NewRetrySink(sk, cfg.Retry.Attempts, cfg.Retry.DelayMS)

	// Build and run indexer
	idx := indexer.New(cfg, client, sk)
	if err := idx.Run(ctx); err != nil {
		s.markJobError(jobID, err)
		return
	}

	// Success
	s.mu.Lock()
	entry.status.Status = "finished"
	finished := time.Now()
	entry.status.FinishedAt = &finished
	s.mu.Unlock()
}

// getJob handles GET /jobs/{id}
func (s *Server) getJob(w http.ResponseWriter, r *http.Request, id string) {
	s.mu.RLock()
	entry, ok := s.jobs[id]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry.status)
}

// cancelJob handles DELETE /jobs/{id}
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, id string) {
	s.mu.Lock()
	entry, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if entry.cancel != nil {
		entry.cancel()
	}

	s.mu.Lock()
	entry.status.Status = "cancelled"
	finished := time.Now()
	entry.status.FinishedAt = &finished
	s.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// markJobError sets the status of the job to error with the provided err.
func (s *Server) markJobError(jobID string, err error) {
	logrus.Errorf("job %s failed: %v", jobID, err)
	s.mu.Lock()
	if entry, ok := s.jobs[jobID]; ok {
		entry.status.Status = "error"
		entry.status.Error = err.Error()
		finished := time.Now()
		entry.status.FinishedAt = &finished
	}
	s.mu.Unlock()
}

// buildConfigFromRequest converts the HTTP request into a validated *config.Config
// replicating the logic from config.Load but without reading from disk.
func buildConfigFromRequest(req JobRequest) (*config.Config, error) {
	// Copy over values
	cfg := &config.Config{
		RPCURL:     req.RPCURL,
		StartBlock: req.StartBlock,
		Contracts:  req.Contracts,
		Storage:    req.Storage,
		Retry:      req.Retry,
		ChunkSize:  req.ChunkSize,
	}

	// Apply defaults
	if cfg.Retry.Attempts == 0 {
		cfg.Retry.Attempts = 3
	}
	if cfg.Retry.DelayMS == 0 {
		cfg.Retry.DelayMS = 1500
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 1_000
	}

	// Validate
	if cfg.RPCURL == "" {
		return nil, fmt.Errorf("rpc_url is required")
	}

	switch cfg.Storage.Type {
	case "csv":
		if cfg.Storage.CSV.OutputDir == "" {
			return nil, fmt.Errorf("storage.csv.output_dir is required")
		}
	case "mysql":
		if cfg.Storage.MySQL.DSN == "" {
			return nil, fmt.Errorf("storage.mysql.dsn is required")
		}
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}

	if len(cfg.Contracts) == 0 {
		return nil, fmt.Errorf("at least one contract must be defined")
	}

	// Parse ABIs
	for i, c := range cfg.Contracts {
		if c.Name == "" {
			return nil, fmt.Errorf("contract at index %d missing name", i)
		}
		if c.Address == "" {
			return nil, fmt.Errorf("contract '%s' missing address", c.Name)
		}
		if c.ABI == "" {
			return nil, fmt.Errorf("contract '%s' missing abi path", c.Name)
		}

		if err := parseABIFile(&cfg.Contracts[i]); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// parseABIFile loads and parses the ABI JSON file specified in the contract config.
func parseABIFile(c *config.ContractConfig) error {
	abiBytes, err := os.ReadFile(c.ABI)
	if err != nil {
		return fmt.Errorf("failed to read abi file for contract '%s': %w", c.Name, err)
	}
	parsed, err := abi.JSON(bytes.NewReader(abiBytes))
	if err != nil {
		return fmt.Errorf("failed to parse abi for contract '%s': %w", c.Name, err)
	}
	c.ParsedABI = &parsed
	return nil
}

// newUUID generates a 32-hex character random ID (not RFC4122 but good enough for internal use).
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
} 