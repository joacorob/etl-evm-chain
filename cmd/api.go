package main

import (
    "os"

    "etl-web3/internal/api"

    "github.com/sirupsen/logrus"
)

func main() {
    port := os.Getenv("API_PORT")
    if port == "" {
        port = "8080"
    }

    srv := api.NewServer()
    logrus.Infof("API server listening on :%s", port)
    if err := srv.Run(port); err != nil {
        logrus.Fatalf("server stopped with error: %v", err)
    }
} 