package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"etl-web3/internal/config"
	"etl-web3/internal/indexer"
	"etl-web3/internal/rpc"
	"etl-web3/internal/sink"

	"github.com/sirupsen/logrus"
)

func main() {
    configPath := flag.String("config", "config.yaml", "Path to configuration file")
    flag.Parse()

    // Configure global logger (timestamped, info level by default).
    logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

    // Load configuration file.
    cfg, err := config.Load(*configPath)
    if err != nil {
        log.Fatalf("failed to load config: %v", err)
    }

    // Prepare cancellable context that listens to OS signals (Ctrl+C).
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-sigCh
        logrus.Info("interrupt received, shutting down gracefully…")
        cancel()
    }()

    // Initialise RPC client with retry logic.
    client, err := rpc.Dial(ctx, cfg.RPCURL, cfg.Retry)
    if err != nil {
        log.Fatalf("failed to connect to RPC: %v", err)
    }

    // Build sink based on configuration.
    var sk sink.Sink
    switch cfg.Storage.Type {
    case "csv":
        s, err := sink.NewCSVSink(cfg.Storage.CSV.OutputDir)
        if err != nil {
            log.Fatalf("failed to initialise csv sink: %v", err)
        }
        sk = s
    case "mysql":
        // Placeholder until MySQL sink is implemented.
        logrus.Warn("mysql sink selected but not yet implemented – proceeding without sink")
    default:
        log.Fatalf("unsupported storage type: %s", cfg.Storage.Type)
    }

    // Wrap the chosen sink with automatic retry logic (if any).
    sk = sink.NewRetrySink(sk, cfg.Retry.Attempts, cfg.Retry.DelayMS)

    // Build and run indexer with the chosen sink.
    idx := indexer.New(cfg, client, sk)
    if err := idx.Run(ctx); err != nil {
        log.Fatalf("indexer terminated with error: %v", err)
    }
} 