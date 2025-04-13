package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/graduate-work-mirea/data-processor-service/repository"
	"go.uber.org/zap"
)

// DataProcessorService handles data processing logic
type DataProcessorService struct {
	fileRepo    *repository.FileRepository
	rabbitRepo  *repository.RabbitMQRepository
	pythonPath  string
	scriptPath  string
	logger      *zap.SugaredLogger
	cutoffDate  string
	batchSize   int
	consumeTime time.Duration
}

// NewDataProcessorService creates a new DataProcessorService instance
func NewDataProcessorService(
	fileRepo *repository.FileRepository,
	rabbitRepo *repository.RabbitMQRepository,
	pythonPath string,
	scriptPath string,
	cutoffDate string,
	batchSize int,
	consumeTime time.Duration,
	logger *zap.SugaredLogger,
) *DataProcessorService {
	return &DataProcessorService{
		fileRepo:    fileRepo,
		rabbitRepo:  rabbitRepo,
		pythonPath:  pythonPath,
		scriptPath:  scriptPath,
		logger:      logger,
		cutoffDate:  cutoffDate,
		batchSize:   batchSize,
		consumeTime: consumeTime,
	}
}

// ProcessMarketplaceData processes marketplace data from RabbitMQ
func (s *DataProcessorService) ProcessMarketplaceData(ctx context.Context) error {
	s.logger.Info("Starting to process marketplace data")

	// Consume messages from RabbitMQ
	data, err := s.rabbitRepo.ConsumeMessages(ctx, s.batchSize, s.consumeTime)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	if len(data) == 0 {
		s.logger.Info("No new data to process")
		return nil
	}

	s.logger.Infof("Consumed %d messages from RabbitMQ", len(data))

	// Generate timestamp for the raw data file
	timestamp := time.Now().Format("20060102_150405")
	rawFilename := fmt.Sprintf("marketplace_data_%s.json", timestamp)
	rawFilePath := filepath.Join(s.fileRepo.GetRawDataPath(), rawFilename)

	// Save raw data to file
	if err := s.fileRepo.SaveMarketplaceData(data, rawFilePath); err != nil {
		return fmt.Errorf("failed to save raw data: %w", err)
	}

	s.logger.Infof("Saved raw data to %s", rawFilePath)

	// Process data using Python script
	if err := s.runPythonProcessor(rawFilePath); err != nil {
		return fmt.Errorf("failed to process data: %w", err)
	}

	s.logger.Info("Data processing completed successfully")
	return nil
}

// runPythonProcessor runs the Python data processing script
func (s *DataProcessorService) runPythonProcessor(inputFile string) error {
	outputDir := s.fileRepo.GetProcessedDataPath()

	s.logger.Infof("Running Python data processor with input: %s, output: %s", inputFile, outputDir)

	// Prepare command
	cmd := exec.Command(
		s.pythonPath,
		s.scriptPath,
		"--input", inputFile,
		"--output", outputDir,
		"--cutoff", s.cutoffDate,
	)

	// Set up pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Python script: %w", err)
	}

	// Read stdout and stderr
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				s.logger.Info(string(buf[:n]))
			}
			if err != nil {
				break
			}
		}
	}()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				s.logger.Warn(string(buf[:n]))
			}
			if err != nil {
				break
			}
		}
	}()

	// Wait for command to finish
	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			s.logger.Errorf("Python script exited with code %d", exitErr.ExitCode())
		}
		return fmt.Errorf("Python script failed: %w", err)
	}

	// Check if processed data files exist
	processedDataFile := filepath.Join(outputDir, "train_data.csv")
	if _, err := os.Stat(processedDataFile); os.IsNotExist(err) {
		return fmt.Errorf("processed data file not created: %s", processedDataFile)
	}

	s.logger.Info("Python data processing completed successfully")
	return nil
}
