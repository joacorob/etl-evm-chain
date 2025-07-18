# Web3 ETL – EVM Event Indexer written in Go

`web3-etl` is an extendable **Extract-Transform-Load (ETL)** pipeline that reads on-chain **event logs** emitted by any EVM compatible smart-contract, enriches them with useful metadata and stores the results in either **CSV files** or a **MySQL** database.

The project is heavily inspired by [chainbound/apollo](https://github.com/chainbound/apollo) but adds:

- Chunk-based log retrieval (no full block downloads)
- Optional event filtering at the RPC level
- Pluggable sinks: CSV or MySQL
- Automatic resume via a progress file
- A lightweight REST API to launch and monitor indexing jobs at runtime

---

## Table of Contents

1. [Project Layout](#project-layout)
2. [Features](#features)
3. [Configuration](#configuration)
4. [Quick Start (CLI)](#quick-start-cli)
5. [REST API](#rest-api)
6. [Storage Back-ends](#storage-back-ends)
7. [Resume Capability](#resume-capability)
8. [Logging & Retry](#logging--retry)
9. [Manual Test Checklist](#manual-test-checklist)
10. [Roadmap](#roadmap)
11. [License](#license)

---

## Project Layout

```text
etl-blockchain/
├── cmd/
│   ├── indexer.go   # CLI launcher
│   └── api.go       # REST server bootstrap
├── internal/
│   ├── api/         # Router, handlers, DTOs
│   ├── config/      # YAML loader & validation
│   ├── indexer/     # Main orchestrator
│   ├── parser/      # ABI decoding & enrichment
│   ├── rpc/         # Resilient Ethereum RPC client
│   └── sink/        # CSV / MySQL back-ends
├── abi/             # Contract ABIs referenced in the config
├── data/            # Generated CSV files (git-ignored)
├── config.yaml.example
└── README.md        # You are here 👋
```

---

## Features

- **Chunked Log Scanning** – Reads logs in fixed-size block windows to avoid timeouts and memory spikes.
- **Event Filtering** – Specify a list of event names per contract; the RPC node returns only the topics you care about.
- **Multi-contract Support** – Index as many contracts as you wish in a single run.
- **Enrichment Layer** – Each record is augmented with: event name, tx hash, block number, timestamp, sender, etc.
- **Pluggable Sinks** – Out-of-the-box support for CSV and MySQL. New sinks can be added by implementing a tiny interface.
- **Progress Tracking** – Last processed block is stored in `.progress.json`; crashes or restarts continue where they left off.
- **REST API** – Trigger long-running indexing jobs programmatically and query their status.
- **Observability** – Structured logging, retries with back-off, and human-readable progress bars.

---

## Configuration

All runtime options live in `config.yaml` (copy `config.yaml.example` and customise):

```yaml
rpc_url: "https://mainnet.infura.io/v3/YOUR_KEY"
start_block: 12345678
chunk_size: 1000 # Optional – window size in blocks
contracts:
  - name: USDC # Human-friendly label
    address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    abi: "./abi/token.json"
    events: # Optional – filter only these events
      - Transfer
storage:
  type: "csv" # "csv" or "mysql"
  mysql:
    dsn: "user:pass@tcp(127.0.0.1:3306)/mydb"
  csv:
    output_dir: "./data" # Folder must exist
retry:
  attempts: 3
  delay_ms: 1500
```

---

## Quick Start (CLI)

```bash
# Dependencies
go mod download

# Copy & edit configuration
cp config.yaml.example config.yaml
vim config.yaml

# Run the indexer
go run cmd/indexer.go --config=config.yaml
```

CLI flags override the YAML file:

```bash
--config        Path to configuration file (default: ./config.yaml)
--start-block   First block to scan (uint64)
--rpc-url       Alternative RPC endpoint
--storage-type  "csv" or "mysql"
```

---

## REST API

The HTTP server (default port **8080**) lets you create, inspect and cancel jobs.

| Verb   | Endpoint         | Purpose                         |
| ------ | ---------------- | ------------------------------- |
| POST   | `/jobs`          | Launch a new indexing job       |
| GET    | `/jobs/{job_id}` | Get real-time status of a job   |
| DELETE | `/jobs/{job_id}` | (Optional) Cancel a running job |

### Example – Create a Job

```bash
curl -X POST http://localhost:8080/jobs \
     -H 'Content-Type: application/json' \
     -d '{
           "rpc_url": "https://mainnet.infura.io/v3/YOUR_KEY",
           "start_block": 16460000,
           "contracts": [{
             "name": "USDC",
             "address": "0xa0b8…e6eb48",
             "abi": "./abi/token.json",
             "events": ["Transfer"]
           }],
           "storage": { "type": "csv", "csv": { "output_dir": "./data" } }
         }'
```

The response contains a **UUID**:

```json
{ "job_id": "1b0dbe6e-2f1c-4758-ad7d-f5021f3ab206" }
```

### Example – Query Progress

```bash
curl http://localhost:8080/jobs/1b0dbe6e-2f1c-4758-ad7d-f5021f3ab206
```

---

## Storage Back-ends

### CSV

- One file per **`<ContractName>_<EventName>.csv`** (e.g. `USDC_Transfer.csv`).
- Headers are auto-generated on first write.
- Ideal for analytics pipelines or quick Excel exploration.

### MySQL

- One table per event: `event_<event_name>` (e.g. `event_transfer`).
- Column types are inferred from the ABI parameters.
- Perfect for dashboards and ad-hoc SQL queries.

---

## Resume Capability

The indexer writes the most recent processed block to `.progress.json`. On restart it resumes from that block, ensuring at-most-once processing without manual intervention.

---

## Logging & Retry

- Structured logs via `logrus` (or `zap`).
- Automatic retries with configurable attempts/delay for transient RPC and sink errors.
- Concise progress output:
  ```text
  ✓ 182000 → 182999 | events: 48 | 1.3 s
  ```

---

## Manual Test Checklist

1. Index **Transfer** events for USDC on mainnet.
2. Confirm decoded data and CSV persistence.
3. Switch sink to MySQL and validate inserts.
4. Simulate network failure; verify retry and resume logic.

---

## Roadmap

- Webhook sink (push events to external HTTP endpoints)
- Parallel indexing of multiple contracts/events
- Prometheus metrics
- Automatic upload of generated CSVs to S3
- Read-only REST API to serve indexed data

---

## License

Distributed under the MIT License.
