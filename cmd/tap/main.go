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

	"github.com/evalops/siphon/config"
)

func main() {
	var configPath string
	var checkConfigOnly bool
	flag.StringVar(&configPath, "config", "config.yaml", "Path to Tap config file")
	flag.BoolVar(&checkConfigOnly, "check-config", false, "Validate config and exit")
	flag.Parse()

	logger := newLogger(os.Stdout)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}
	if checkConfigOnly {
		logger.Info("config is valid", "path", configPath)
		fmt.Println("config check passed")
		return
	}

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("tap runtime failed", "error", err)
		os.Exit(1)
	}

	logger.Info("siphon stopped")
	fmt.Println("shutdown complete")
}

func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
