package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/graduate-work-mirea/data-processor-service/internal/models"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Database represents a database connection
type Database struct {
	db *sqlx.DB
}

// ProcessedDataRecord represents a record in the processed_data table
type ProcessedDataRecord struct {
	ID           int       `db:"id"`
	RawDataID    int       `db:"raw_data_id"`
	ProcessedData json.RawMessage `db:"processed_data"`
	ProcessedAt  time.Time `db:"processed_at"`
	CreatedAt    time.Time `db:"created_at"`
}

// New creates a new database connection
func New(dsn string) (*Database, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Database{db: db}, nil
}

// Close closes the database connection
func (d *Database) Close() error {
	return d.db.Close()
}

// SaveProcessedData saves processed data to the database
func (d *Database) SaveProcessedData(rawDataID int, data *models.ProcessedData) error {
	jsonData, err := data.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert processed data to JSON: %w", err)
	}

	query := `
		INSERT INTO processed_data (raw_data_id, processed_data)
		VALUES ($1, $2)
		RETURNING id
	`

	var id int
	err = d.db.QueryRow(query, rawDataID, jsonData).Scan(&id)
	if err != nil {
		return fmt.Errorf("failed to save processed data: %w", err)
	}

	return nil
}

// SaveProcessedDataWithoutRawID saves processed data to the database without raw_data_id
// This is useful for MVP where we might not have raw_data table yet
func (d *Database) SaveProcessedDataWithoutRawID(data *models.ProcessedData) error {
	jsonData, err := data.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert processed data to JSON: %w", err)
	}

	query := `
		INSERT INTO processed_data (processed_data)
		VALUES ($1)
		RETURNING id
	`

	var id int
	err = d.db.QueryRow(query, jsonData).Scan(&id)
	if err != nil {
		return fmt.Errorf("failed to save processed data: %w", err)
	}

	return nil
}
