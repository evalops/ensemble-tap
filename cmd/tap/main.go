package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/evalops/ensemble-tap/config"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to Tap config file")
	flag.Parse()

	logger := newLogger(os.Stdout)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("tap runtime failed", "error", err)
		os.Exit(1)
	}

	logger.Info("ensemble-tap stopped")
	fmt.Println("shutdown complete")
}

func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
