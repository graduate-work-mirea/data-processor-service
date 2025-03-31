package models

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

// RawData represents the incoming raw data from RabbitMQ
type RawData struct {
	ProductID string      `json:"product_id"`
	Sales     interface{} `json:"sales"`
	Date      string      `json:"date"`
}

// ProcessedData represents the normalized and processed data
type ProcessedData struct {
	ProductID string    `json:"product_id"`
	Sales     int       `json:"sales"`
	Date      time.Time `json:"date"`
}

// Validate checks if the raw data has all required fields
func (r *RawData) Validate() error {
	if r.ProductID == "" {
		return errors.New("product_id is required")
	}
	if r.Sales == nil {
		return errors.New("sales is required")
	}
	if r.Date == "" {
		return errors.New("date is required")
	}
	return nil
}

// Process normalizes the raw data into processed data
func (r *RawData) Process() (*ProcessedData, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	// Convert sales to int
	var sales int
	switch v := r.Sales.(type) {
	case float64:
		sales = int(v)
	case int:
		sales = v
	case string:
		var err error
		sales, err = strconv.Atoi(v)
		if err != nil {
			return nil, errors.New("invalid sales value: cannot convert to integer")
		}
	default:
		return nil, errors.New("invalid sales type")
	}

	// Parse date to ISO 8601 format
	date, err := time.Parse("2006-01-02", r.Date)
	if err != nil {
		// Try other common formats
		date, err = time.Parse(time.RFC3339, r.Date)
		if err != nil {
			return nil, errors.New("invalid date format: must be YYYY-MM-DD or ISO 8601")
		}
	}

	return &ProcessedData{
		ProductID: r.ProductID,
		Sales:     sales,
		Date:      date,
	}, nil
}

// ToJSON converts ProcessedData to JSON string
func (p *ProcessedData) ToJSON() (string, error) {
	type jsonFormat struct {
		ProductID string `json:"product_id"`
		Sales     int    `json:"sales"`
		Date      string `json:"date"`
	}

	data := jsonFormat{
		ProductID: p.ProductID,
		Sales:     p.Sales,
		Date:      p.Date.Format(time.RFC3339),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}
