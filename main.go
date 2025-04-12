package main

import (
	"context"
	"github.com/graduate-work-mirea/data-processor-service/assembly"
	"github.com/graduate-work-mirea/data-processor-service/config"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

// @title Data Processor Service
// @version 1.0
// @description Process marketplace data for ML predictions
func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	if err := godotenv.Load(); err != nil {
		sugar.Warnf("Error loading .env file: %v", err)
	}

	cfg, err := config.New()
	if err != nil {
		sugar.Fatalf("Failed to load config: %v", err)
	}

	locator, err := assembly.NewServiceLocator(cfg, sugar)
	if err != nil {
		sugar.Fatalf("Failed to initialize service locator: %v", err)
	}
	defer locator.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sugar.Infof("Starting RabbitMQ controller with scheduler interval: %v", cfg.SchedulerInterval)
	locator.RabbitMQController.StartProcessing(ctx, cfg.SchedulerInterval)

	// Wait for termination signal
	sig := <-sigCh
	sugar.Infof("Received signal: %v, shutting down...", sig)

	// Cancel context to stop the scheduler
	cancel()
}
