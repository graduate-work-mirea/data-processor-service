package models

import (
	"testing"
	"time"
)

func TestRawDataValidate(t *testing.T) {
	tests := []struct {
		name    string
		rawData RawData
		wantErr bool
	}{
		{
			name: "Valid data",
			rawData: RawData{
				ProductID: "123",
				Sales:     100,
				Date:      "2024-10-01",
			},
			wantErr: false,
		},
		{
			name: "Missing product_id",
			rawData: RawData{
				ProductID: "",
				Sales:     100,
				Date:      "2024-10-01",
			},
			wantErr: true,
		},
		{
			name: "Missing sales",
			rawData: RawData{
				ProductID: "123",
				Sales:     nil,
				Date:      "2024-10-01",
			},
			wantErr: true,
		},
		{
			name: "Missing date",
			rawData: RawData{
				ProductID: "123",
				Sales:     100,
				Date:      "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rawData.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("RawData.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRawDataProcess(t *testing.T) {
	tests := []struct {
		name    string
		rawData RawData
		want    *ProcessedData
		wantErr bool
	}{
		{
			name: "Valid data with int sales",
			rawData: RawData{
				ProductID: "123",
				Sales:     100,
				Date:      "2024-10-01",
			},
			want: &ProcessedData{
				ProductID: "123",
				Sales:     100,
				Date:      time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "Valid data with float sales",
			rawData: RawData{
				ProductID: "123",
				Sales:     100.5,
				Date:      "2024-10-01",
			},
			want: &ProcessedData{
				ProductID: "123",
				Sales:     100,
				Date:      time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "Valid data with string sales",
			rawData: RawData{
				ProductID: "123",
				Sales:     "100",
				Date:      "2024-10-01",
			},
			want: &ProcessedData{
				ProductID: "123",
				Sales:     100,
				Date:      time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "Valid data with ISO 8601 date",
			rawData: RawData{
				ProductID: "123",
				Sales:     100,
				Date:      "2024-10-01T00:00:00Z",
			},
			want: &ProcessedData{
				ProductID: "123",
				Sales:     100,
				Date:      time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "Invalid sales type",
			rawData: RawData{
				ProductID: "123",
				Sales:     "not a number",
				Date:      "2024-10-01",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid date format",
			rawData: RawData{
				ProductID: "123",
				Sales:     100,
				Date:      "not a date",
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.rawData.Process()
			if (err != nil) != tt.wantErr {
				t.Errorf("RawData.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got.ProductID != tt.want.ProductID {
				t.Errorf("RawData.Process() ProductID = %v, want %v", got.ProductID, tt.want.ProductID)
			}
			if got.Sales != tt.want.Sales {
				t.Errorf("RawData.Process() Sales = %v, want %v", got.Sales, tt.want.Sales)
			}
			if !got.Date.Equal(tt.want.Date) {
				t.Errorf("RawData.Process() Date = %v, want %v", got.Date, tt.want.Date)
			}
		})
	}
}
