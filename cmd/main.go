package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/graduate-work-mirea/data-processor-service/internal/config"
	"github.com/graduate-work-mirea/data-processor-service/internal/db"
	"github.com/graduate-work-mirea/data-processor-service/internal/processor"
	"github.com/graduate-work-mirea/data-processor-service/internal/rabbitmq"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Connect to database
	database, err := db.New(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	// Connect to RabbitMQ
	rabbitMQ, err := rabbitmq.New(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitMQ.Close()

	// Create processor
	proc := processor.New(database, rabbitMQ)

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Start processor in a goroutine
	go func() {
		if err := proc.Start(); err != nil {
			log.Fatalf("Failed to start processor: %v", err)
		}
	}()

	log.Println("Data Processor Service started")

	// Wait for shutdown signal
	<-shutdown
	log.Println("Shutting down...")
}
