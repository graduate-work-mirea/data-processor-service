package config

import (
	"errors"
	"os"
)

// Config represents the application configuration
type Config struct {
	RabbitMQURL string
	PostgresDSN string
}

// Load loads the configuration from environment variables
func Load() (*Config, error) {
	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		return nil, errors.New("RABBITMQ_URL environment variable is required")
	}

	postgresDSN := os.Getenv("POSTGRES_DSN")
	if postgresDSN == "" {
		return nil, errors.New("POSTGRES_DSN environment variable is required")
	}

	return &Config{
		RabbitMQURL: rabbitMQURL,
		PostgresDSN: postgresDSN,
	}, nil
}
