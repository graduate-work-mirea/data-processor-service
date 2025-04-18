package config

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	RabbitMQURL           string
	DataQueueName         string
	SchedulerInterval     time.Duration
	DataPath              string
	ScriptsPath           string
	PythonPath            string
	CutoffDate            string
	BatchSize             int
	ConsumeTimeoutSeconds int
	// PostgreSQL configuration
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDBName   string
	PostgresSSLMode  string
}

func New() (*Config, error) {
	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}

	dataQueueName := os.Getenv("DATA_QUEUE_NAME")
	if dataQueueName == "" {
		dataQueueName = "marketplace_data"
	}

	schedulerIntervalStr := os.Getenv("SCHEDULER_INTERVAL_HOURS")
	schedulerInterval := 24 * time.Hour // Default: once per day
	if schedulerIntervalStr != "" {
		interval, err := strconv.Atoi(schedulerIntervalStr)
		if err == nil && interval > 0 {
			schedulerInterval = time.Duration(interval) * time.Hour
		}
	}

	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "./data"
	}

	scriptsPath := os.Getenv("SCRIPTS_PATH")
	if scriptsPath == "" {
		// Default to the scripts directory relative to the executable
		execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		scriptsPath = filepath.Join(execDir, "scripts")
	}

	pythonPath := os.Getenv("PYTHON_PATH")
	if pythonPath == "" {
		pythonPath = "python"
	}

	cutoffDate := os.Getenv("CUTOFF_DATE")
	if cutoffDate == "" {
		cutoffDate = "2025-03-20"
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	batchSize := 1000 // Default batch size
	if batchSizeStr != "" {
		size, err := strconv.Atoi(batchSizeStr)
		if err == nil && size > 0 {
			batchSize = size
		}
	}

	consumeTimeoutStr := os.Getenv("CONSUME_TIMEOUT_SECONDS")
	consumeTimeout := 60 // Default timeout in seconds
	if consumeTimeoutStr != "" {
		timeout, err := strconv.Atoi(consumeTimeoutStr)
		if err == nil && timeout > 0 {
			consumeTimeout = timeout
		}
	}

	// PostgreSQL configuration
	postgresHost := os.Getenv("POSTGRES_HOST")
	if postgresHost == "" {
		postgresHost = "localhost"
	}

	postgresPort := os.Getenv("POSTGRES_PORT")
	if postgresPort == "" {
		postgresPort = "5432"
	}

	postgresUser := os.Getenv("POSTGRES_USER")
	if postgresUser == "" {
		postgresUser = "postgres"
	}

	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	if postgresPassword == "" {
		postgresPassword = "postgres"
	}

	postgresDBName := os.Getenv("POSTGRES_DB_NAME")
	if postgresDBName == "" {
		postgresDBName = "marketplace_data"
	}

	postgresSSLMode := os.Getenv("POSTGRES_SSL_MODE")
	if postgresSSLMode == "" {
		postgresSSLMode = "disable"
	}

	return &Config{
		RabbitMQURL:           rabbitMQURL,
		DataQueueName:         dataQueueName,
		SchedulerInterval:     schedulerInterval,
		DataPath:              dataPath,
		ScriptsPath:           scriptsPath,
		PythonPath:            pythonPath,
		CutoffDate:            cutoffDate,
		BatchSize:             batchSize,
		ConsumeTimeoutSeconds: consumeTimeout,
		PostgresHost:          postgresHost,
		PostgresPort:          postgresPort,
		PostgresUser:          postgresUser,
		PostgresPassword:      postgresPassword,
		PostgresDBName:        postgresDBName,
		PostgresSSLMode:       postgresSSLMode,
	}, nil
}
