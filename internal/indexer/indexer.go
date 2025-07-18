package indexer

import (
	"context"
	"math/big"
	"time"

	"etl-web3/internal/config"
	"etl-web3/internal/parser"
	"etl-web3/internal/rpc"
	"etl-web3/internal/sink"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

// DefaultChunkSize defines how many blocks will be scanned in a single RPC call.
// This is currently hard-coded but can become configurable through CLI flags or
// the main config file later on.
const DefaultChunkSize uint64 = 1_000

// Indexer orchestrates the end-to-end ETL process.
// It is intentionally decoupled from concrete parser / sink implementations so
// those components can evolve independently.
type Indexer struct {
    cfg       *config.Config
    client    *rpc.Client
    sink      sink.Sink
    chunkSize uint64
    parser    *parser.Parser

    // Filtering helpers
    filteredAddresses  []common.Address   // addresses with event filters applied
    unfilteredAddresses []common.Address  // addresses without filters (all events fetched)
    filteredTopics     []common.Hash      // precomputed topic0 hashes for the allowed events

    // Pre-computed helpers to speed things up during the scan loop.
    contractByAddress map[common.Address]config.ContractConfig // quick look-up
    addresses         []common.Address                         // slice reused in filter queries
}

// New constructs a fully-initialised Indexer.
//
// The caller is responsible for creating the RPC client and the desired Sink
// implementation so different configurations (e.g. mock sink for tests) can be
// injected as needed.
func New(cfg *config.Config, client *rpc.Client, sk sink.Sink) *Indexer {
    m := make(map[common.Address]config.ContractConfig, len(cfg.Contracts))
    addrs := make([]common.Address, 0, len(cfg.Contracts))

    // Helpers for filtering
    var filteredAddrs []common.Address
    var unfilteredAddrs []common.Address
    topicSet := make(map[common.Hash]struct{})

    for _, c := range cfg.Contracts {
        addr := common.HexToAddress(c.Address)
        m[addr] = c
        addrs = append(addrs, addr)

        if len(c.Events) > 0 {
            filteredAddrs = append(filteredAddrs, addr)

            // Pre-compute topic0 (event signature hash) for every configured event name.
            if c.ParsedABI != nil {
                for _, evName := range c.Events {
                    evDef, ok := c.ParsedABI.Events[evName]
                    if !ok {
                        // If event not found in ABI, panic is avoided; instead log and continue.
                        logrus.Warnf("event '%s' not found in ABI for contract '%s'", evName, c.Name)
                        continue
                    }
                    topicSet[evDef.ID] = struct{}{}
                }
            }
        } else {
            unfilteredAddrs = append(unfilteredAddrs, addr)
        }
    }

    // Convert topicSet to slice.
    topics := make([]common.Hash, 0, len(topicSet))
    for h := range topicSet {
        topics = append(topics, h)
    }

    // Use chunk size from config if provided, otherwise fall back to built-in default.
    size := cfg.ChunkSize
    if size == 0 {
        size = DefaultChunkSize
    }
    if cfg.StartBlock == 0 {
        // Prevent infinite loops if start block is somehow zero in config.
        cfg.StartBlock = 1
    }

    pr := parser.New(cfg, client)

    return &Indexer{
        cfg:               cfg,
        client:            client,
        sink:              sk,
        chunkSize:         size,
        contractByAddress: m,
        addresses:         addrs,
        parser:            pr,

        filteredAddresses:  filteredAddrs,
        unfilteredAddresses: unfilteredAddrs,
        filteredTopics:     topics,
    }
}

// Run starts the indexing loop and blocks until the context is cancelled or an
// unrecoverable error is returned.
func (idx *Indexer) Run(ctx context.Context) error {
    // Fetch latest block number (cheap RPC) so we know up to where we need to scan.
    latest, err := idx.client.LatestBlockNumber(ctx)
    if err != nil {
        return err
    }

    from := idx.cfg.StartBlock

    logrus.Infof("Starting indexer | from=%d latest=%d chunkSize=%d", from, latest, idx.chunkSize)

    for from <= latest {
        // Respect context cancellation early.
        if err := ctx.Err(); err != nil {
            return err
        }

        to := from + idx.chunkSize - 1
        if to > latest {
            to = latest
        }

        start := time.Now()
        evCount, err := idx.processRange(ctx, from, to)
        if err != nil {
            return err
        }
        elapsed := time.Since(start).Seconds()

        logrus.Infof("[OK] Block %d → %d | Events: %d | Time: %.2fs", from, to, evCount, elapsed)

        // Move window forward.
        from = to + 1
    }

    return nil
}

// processRange fetches, parses and persists logs within the [from, to] block
// interval (inclusive). It returns the number of events successfully written to
// the sink.
func (idx *Indexer) processRange(ctx context.Context, from, to uint64) (int, error) {
    var logs []types.Log

    // 1. Addresses with explicit event filters
    if len(idx.filteredAddresses) > 0 {
        if len(idx.filteredTopics) == 0 {
            // No valid topics resolved; treat as unfiltered to avoid empty filter resulting in no logs.
            query := ethereum.FilterQuery{
                FromBlock: big.NewInt(int64(from)),
                ToBlock:   big.NewInt(int64(to)),
                Addresses: idx.filteredAddresses,
            }
            lgs, err := idx.client.GetLogs(ctx, query)
            if err != nil {
                return 0, err
            }
            logs = append(logs, lgs...)
        } else {
            query := ethereum.FilterQuery{
                FromBlock: big.NewInt(int64(from)),
                ToBlock:   big.NewInt(int64(to)),
                Addresses: idx.filteredAddresses,
                Topics:    [][]common.Hash{idx.filteredTopics},
            }
            lgs, err := idx.client.GetLogs(ctx, query)
            if err != nil {
                return 0, err
            }
            logs = append(logs, lgs...)
        }
    }

    // 2. Addresses without filters (fetch all events)
    if len(idx.unfilteredAddresses) > 0 {
        query := ethereum.FilterQuery{
            FromBlock: big.NewInt(int64(from)),
            ToBlock:   big.NewInt(int64(to)),
            Addresses: idx.unfilteredAddresses,
        }
        lgs, err := idx.client.GetLogs(ctx, query)
        if err != nil {
            return 0, err
        }
        logs = append(logs, lgs...)
    }

    eventsWritten := 0
    for _, lg := range logs {
        evt, err := idx.parser.Parse(ctx, &lg)
        if err != nil {
            // Non-fatal: continue processing other logs but report at debug level.
            logrus.Debugf("failed to parse log | block=%d tx=%s err=%v", lg.BlockNumber, lg.TxHash.Hex(), err)
            continue
        }

        if idx.sink != nil {
            if err := idx.sink.Write(evt); err != nil {
                // Propagate error so higher-level retry mechanism can kick in.
                return eventsWritten, err
            }
        }

        eventsWritten++
    }

    return eventsWritten, nil
} 