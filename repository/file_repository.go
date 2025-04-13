package repository

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FileRepository handles file operations
type FileRepository struct {
	baseDataPath string
}

// NewFileRepository creates a new FileRepository instance
func NewFileRepository(baseDataPath string) *FileRepository {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDataPath, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create data directory: %v", err))
	}

	return &FileRepository{
		baseDataPath: baseDataPath,
	}
}

// SaveMarketplaceData saves marketplace data to a JSON file
func (r *FileRepository) SaveMarketplaceData(data []map[string]interface{}, filePath string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Marshal data to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// GetProcessedDataPath returns the path to the processed data directory
func (r *FileRepository) GetProcessedDataPath() string {
	processedPath := filepath.Join(r.baseDataPath, "processed")

	// Create directory if it doesn't exist
	if err := os.MkdirAll(processedPath, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create processed data directory: %v", err))
	}

	return processedPath
}

// GetRawDataPath returns the path to the raw data directory
func (r *FileRepository) GetRawDataPath() string {
	rawPath := filepath.Join(r.baseDataPath, "raw")

	// Create directory if it doesn't exist
	if err := os.MkdirAll(rawPath, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create raw data directory: %v", err))
	}

	return rawPath
}
