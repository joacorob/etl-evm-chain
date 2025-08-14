package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/ethereum/go-ethereum/accounts/abi"

	yaml "gopkg.in/yaml.v2"
)

type ContractConfig struct {
    Name      string     `yaml:"name"`
    Address   string     `yaml:"address"`
    ABI       string     `yaml:"abi"`
    ParsedABI *abi.ABI   `yaml:"-"`
    Events    []string   `yaml:"events"`
}

type StorageConfig struct {
    Type  string `yaml:"type"`
    MySQL struct {
        DSN string `yaml:"dsn"`
    } `yaml:"mysql"`
    CSV struct {
        OutputDir string `yaml:"output_dir"`
    } `yaml:"csv"`
}

type RetryConfig struct {
    Attempts int `yaml:"attempts"`
    DelayMS  int `yaml:"delay_ms"`
}

type Config struct {
    RPCURL     string           `yaml:"rpc_url"`
    StartBlock uint64           `yaml:"start_block"`
    Contracts  []ContractConfig `yaml:"contracts"`
    Storage    StorageConfig    `yaml:"storage"`
    Retry      RetryConfig      `yaml:"retry"`
    // ChunkSize defines how many blocks will be processed per batch when fetching logs.
    // If not set, a sensible default will be applied by the loader.
    ChunkSize  uint64           `yaml:"chunk_size"`
    // Workers defines how many concurrent workers will process block ranges.
    // If not set, it defaults to the number of available CPUs.
    Workers    int              `yaml:"workers"`
}

// Load reads and unmarshals the configuration file located at the given path.
func Load(path string) (*Config, error) {
    absPath, err := filepath.Abs(path)
    if err != nil {
        return nil, err
    }

    data, err := ioutil.ReadFile(absPath)
    if err != nil {
        return nil, err
    }

    var cfg Config
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        return nil, err
    }

    // Basic validation
    if cfg.RPCURL == "" {
        return nil, fmt.Errorf("rpc_url is required")
    }

    // Validate storage configuration
    switch cfg.Storage.Type {
    case "mysql":
        if cfg.Storage.MySQL.DSN == "" {
            return nil, fmt.Errorf("storage.mysql.dsn is required when storage type is mysql")
        }
    case "csv":
        if cfg.Storage.CSV.OutputDir == "" {
            return nil, fmt.Errorf("storage.csv.output_dir is required when storage type is csv")
        }
    default:
        return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
    }

    // Ensure we have at least one contract
    if len(cfg.Contracts) == 0 {
        return nil, fmt.Errorf("at least one contract must be defined")
    }

    // Directory of the config file to resolve relative paths
    cfgDir := filepath.Dir(absPath)

    // Load and parse ABI for each contract
    for i, c := range cfg.Contracts {
        if c.Name == "" {
            return nil, fmt.Errorf("contract at index %d is missing name", i)
        }
        if c.Address == "" {
            return nil, fmt.Errorf("contract '%s' is missing address", c.Name)
        }
        if c.ABI == "" {
            return nil, fmt.Errorf("contract '%s' is missing abi path", c.Name)
        }

        abiPath := c.ABI
        if !filepath.IsAbs(abiPath) {
            abiPath = filepath.Join(cfgDir, abiPath)
        }

        // Verify file exists
        if _, err := os.Stat(abiPath); err != nil {
            return nil, fmt.Errorf("abi file for contract '%s' not found: %w", c.Name, err)
        }

        abiBytes, err := ioutil.ReadFile(abiPath)
        if err != nil {
            return nil, fmt.Errorf("failed to read abi file for contract '%s': %w", c.Name, err)
        }

        parsed, err := abi.JSON(bytes.NewReader(abiBytes))
        if err != nil {
            return nil, fmt.Errorf("failed to parse abi for contract '%s': %w", c.Name, err)
        }

        cfg.Contracts[i].ParsedABI = &parsed
        // Replace ABI path with absolute path for future reference
        cfg.Contracts[i].ABI = abiPath
    }

    // Default retry values if not set
    if cfg.Retry.Attempts == 0 {
        cfg.Retry.Attempts = 3
    }
    if cfg.Retry.DelayMS == 0 {
        cfg.Retry.DelayMS = 1500
    }

    // Apply default chunk size if not specified (allows backward-compatible configs).
    if cfg.ChunkSize == 0 {
        cfg.ChunkSize = 1_000
    }

    // Default workers to the number of CPUs when not provided or invalid.
    if cfg.Workers <= 0 {
        cfg.Workers = runtime.NumCPU()
        if cfg.Workers < 1 {
            cfg.Workers = 1
        }
    }

    return &cfg, nil
} 