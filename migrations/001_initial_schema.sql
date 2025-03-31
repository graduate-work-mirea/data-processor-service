-- Create raw_data table
CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create processed_data table
CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    raw_data_id INT REFERENCES raw_data(id),
    processed_data JSONB NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on raw_data_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_processed_data_raw_data_id ON processed_data(raw_data_id);

-- Create index on processed_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_processed_data_processed_at ON processed_data(processed_at);
