package api

import (
	"time"

	"etl-web3/internal/config"
)

// JobRequest mirrors the structure of config.Config but is tagged for JSON
// decoding so it can be received directly from HTTP requests.
type JobRequest struct {
    RPCURL     string                    `json:"rpc_url"`
    StartBlock uint64                    `json:"start_block"`
    Contracts  []config.ContractConfig   `json:"contracts"`
    Storage    config.StorageConfig      `json:"storage"`
    Retry      config.RetryConfig        `json:"retry"`
    ChunkSize  uint64                    `json:"chunk_size"`
}

// JobResponse is returned after a successful job creation.
type JobResponse struct {
    JobID string `json:"job_id"`
}

// JobStatus represents the runtime state of a launched job.
type JobStatus struct {
    JobID      string     `json:"job_id"`
    Status     string     `json:"status"` // queued | running | finished | error | cancelled
    Error      string     `json:"error,omitempty"`
    StartedAt  time.Time  `json:"started_at,omitempty"`
    FinishedAt *time.Time `json:"finished_at,omitempty"`
} 