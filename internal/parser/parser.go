package parser

import (
	"context"
	"fmt"
	"math/big"

	"etl-web3/internal/config"
	"etl-web3/internal/rpc"
	"etl-web3/internal/sink"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Parser handles the transformation of raw Ethereum logs into generic
// sink.Event maps enriched with additional metadata such as timestamps and
// transaction senders.
type Parser struct {
    client    *rpc.Client
    contracts map[common.Address]config.ContractConfig
    chainID   *big.Int
    // timestampCache allows reusing block timestamps when multiple events
    // belong to the same block, saving additional RPC calls.
    timestampCache map[uint64]uint64
}

// New builds a Parser using the loaded configuration and an initialised RPC
// client. The ABI of every configured contract is cached for quick look-ups.
func New(cfg *config.Config, client *rpc.Client) *Parser {
    m := make(map[common.Address]config.ContractConfig, len(cfg.Contracts))
    for _, c := range cfg.Contracts {
        m[common.HexToAddress(c.Address)] = c
    }
    return &Parser{client: client, contracts: m, timestampCache: make(map[uint64]uint64)}
}

// Parse converts the provided log into a sink.Event. When the contract ABI is
// available, the event parameters are fully decoded; otherwise a minimal event
// containing only generic information is returned.
func (p *Parser) Parse(ctx context.Context, lg *types.Log) (sink.Event, error) {
    evt := sink.Event{
        "tx_hash":       lg.TxHash.Hex(),
        "block_number":  lg.BlockNumber,
        "contract":      lg.Address.Hex(),
        "contract_name": "unknown",
        "event_name":    "unknown",
        "chain_id":      "",
    }

    cfg, ok := p.contracts[lg.Address]
    if !ok || cfg.ParsedABI == nil {
        if ok {
            evt["contract_name"] = cfg.Name
        }
        // No ABI for this address – return minimal info so it is not lost.
        p.enrichWithBlockAndTx(ctx, lg, evt)
        return evt, nil
    }

    // Derive event definition via its signature hash (topic[0]).
    evDef, err := p.findEventByID(cfg.ParsedABI, lg.Topics[0])
    if err != nil {
        return evt, err
    }
    evt["event_name"] = evDef.Name
    // Store the human-friendly contract name for downstream sinks (e.g. CSV naming).
    evt["contract_name"] = cfg.Name

    // Decode non-indexed params (contained in log.Data).
    args := make(map[string]interface{})
    if err := cfg.ParsedABI.UnpackIntoMap(args, evDef.Name, lg.Data); err != nil {
        return evt, err
    }

    // Decode indexed params (topics[1:]).
    var indexedArgs abi.Arguments
    for _, input := range evDef.Inputs {
        if input.Indexed {
            indexedArgs = append(indexedArgs, input)
        }
    }

    for i, arg := range indexedArgs {
        if len(lg.Topics) <= i+1 {
            break
        }

        topicVals := make(map[string]interface{})
        // ParseTopicsIntoMap mutates the provided map and returns only error.
        err := abi.ParseTopicsIntoMap(topicVals, abi.Arguments{arg}, []common.Hash{lg.Topics[i+1]})
        if err == nil {
            for k, v := range topicVals {
                args[k] = v
            }
        } else {
            // On failure, keep raw topic so data is not discarded.
            args[arg.Name] = lg.Topics[i+1].Hex()
        }
    }

    // Merge decoded params into the event map.
    for k, v := range args {
        evt[k] = v
    }

    // Extra metadata (timestamp, tx_from).
    p.enrichWithBlockAndTx(ctx, lg, evt)

    return evt, nil
}

// enrichWithBlockAndTx adds timestamp and tx_from metadata using best-effort
// RPC calls. Failures are silently ignored so they do not block main parsing.
func (p *Parser) enrichWithBlockAndTx(ctx context.Context, lg *types.Log, evt sink.Event) {
    // Block timestamp (with cache to avoid repeated RPC calls).
    if ts, ok := p.timestampCache[lg.BlockNumber]; ok {
        evt["timestamp"] = ts
    } else if hdr, err := p.client.GetHeaderByNumber(ctx, big.NewInt(int64(lg.BlockNumber))); err == nil {
        evt["timestamp"] = hdr.Time
        p.timestampCache[lg.BlockNumber] = hdr.Time
    }

    // Transaction sender.
    if p.chainID == nil {
        if id, err := p.client.NetworkID(ctx); err == nil {
            p.chainID = id
        }
    }
    // Include chain_id in event once it is known.
    if p.chainID != nil {
        evt["chain_id"] = p.chainID.String()
    }
    if p.chainID != nil {
        if tx, _, err := p.client.Client.TransactionByHash(ctx, lg.TxHash); err == nil {
            signer := types.LatestSignerForChainID(p.chainID)
            if from, err := types.Sender(signer, tx); err == nil {
                evt["tx_from"] = from.Hex()
            }
        }
    }
}

// findEventByID searches the ABI for an event whose ID matches the provided
// signature hash.
func (p *Parser) findEventByID(contractABI *abi.ABI, id common.Hash) (*abi.Event, error) {
    for _, ev := range contractABI.Events {
        if ev.ID == id {
            return &ev, nil
        }
    }
    return nil, fmt.Errorf("event with ID %s not found in ABI", id.Hex())
} 