package rpc

import (
	"context"
	"math/big"
	"time"

	"etl-web3/internal/config"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethereum/go-ethereum/ethclient"
)

// Client wraps the go-ethereum ethclient with potential additional helpers.
type Client struct {
    *ethclient.Client

    retryCfg config.RetryConfig
}

// Dial establishes a new RPC connection with retry support using the provided context and URL.
// The retry configuration controls the number of attempts and the delay (in milliseconds) between them.
func Dial(ctx context.Context, url string, retryCfg config.RetryConfig) (*Client, error) {
    if retryCfg.Attempts == 0 {
        retryCfg.Attempts = 3
    }
    if retryCfg.DelayMS == 0 {
        retryCfg.DelayMS = 1500
    }

    var (
        cli *ethclient.Client
        err error
    )

    for attempt := 1; attempt <= retryCfg.Attempts; attempt++ {
        cli, err = ethclient.DialContext(ctx, url)
        if err == nil {
            return &Client{Client: cli, retryCfg: retryCfg}, nil
        }

        logrus.Warnf("RPC dial failed (attempt %d/%d): %v", attempt, retryCfg.Attempts, err)

        // Don't wait after the final attempt
        if attempt < retryCfg.Attempts {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(time.Duration(retryCfg.DelayMS) * time.Millisecond):
            }
        }
    }

    return nil, err
}

// GetBlockByNumber retrieves a block by its number with retry logic.
// Pass nil as the number parameter to fetch the latest block.
func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
    var (
        block *types.Block
        err   error
    )

    for attempt := 1; attempt <= c.retryCfg.Attempts; attempt++ {
        block, err = c.Client.BlockByNumber(ctx, number)
        if err == nil {
            // DEBUG: print transaction types within the fetched block
            logrus.Infof("Processing block %d with %d txs", block.NumberU64(), len(block.Transactions()))
            for i, tx := range block.Transactions() {
                logrus.Infof("TX %d type: %d", i, tx.Type())
            }
            return block, nil
        }

        logrus.Warnf("GetBlockByNumber failed (attempt %d/%d): %v", attempt, c.retryCfg.Attempts, err)

        if attempt < c.retryCfg.Attempts {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(time.Duration(c.retryCfg.DelayMS) * time.Millisecond):
            }
        }
    }

    return nil, err
}

// GetLogs fetches logs that match the given filter query with retry logic.
func (c *Client) GetLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
    var (
        logs []types.Log
        err  error
    )

    for attempt := 1; attempt <= c.retryCfg.Attempts; attempt++ {
        logs, err = c.Client.FilterLogs(ctx, query)
        if err == nil {
            return logs, nil
        }

        logrus.Warnf("GetLogs failed (attempt %d/%d): %v", attempt, c.retryCfg.Attempts, err)

        if attempt < c.retryCfg.Attempts {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(time.Duration(c.retryCfg.DelayMS) * time.Millisecond):
            }
        }
    }

    return nil, err
}

// GetHeaderByNumber retrieves a block header by its number with retry logic.
// Pass nil as the number parameter to fetch the latest header. This is a
// lightweight alternative to fetching the full block and is useful when only
// the timestamp or basic metadata is required.
func (c *Client) GetHeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
    var (
        header *types.Header
        err    error
    )

    for attempt := 1; attempt <= c.retryCfg.Attempts; attempt++ {
        header, err = c.Client.HeaderByNumber(ctx, number)
        if err == nil {
            return header, nil
        }

        logrus.Warnf("GetHeaderByNumber failed (attempt %d/%d): %v", attempt, c.retryCfg.Attempts, err)

        if attempt < c.retryCfg.Attempts {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(time.Duration(c.retryCfg.DelayMS) * time.Millisecond):
            }
        }
    }

    return nil, err
}

// LatestBlockNumber fetches the latest block number via eth_blockNumber with
// retry logic. It is significantly cheaper than downloading the full latest
// block when only the height is required.
func (c *Client) LatestBlockNumber(ctx context.Context) (uint64, error) {
    var (
        num uint64
        err error
    )

    for attempt := 1; attempt <= c.retryCfg.Attempts; attempt++ {
        num, err = c.Client.BlockNumber(ctx)
        if err == nil {
            return num, nil
        }

        logrus.Warnf("LatestBlockNumber failed (attempt %d/%d): %v", attempt, c.retryCfg.Attempts, err)

        if attempt < c.retryCfg.Attempts {
            select {
            case <-ctx.Done():
                return 0, ctx.Err()
            case <-time.After(time.Duration(c.retryCfg.DelayMS) * time.Millisecond):
            }
        }
    }

    return 0, err
} 