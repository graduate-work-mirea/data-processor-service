-- Create processed_data table
CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    region VARCHAR(100) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sales_quantity DECIMAL NOT NULL,
    price DECIMAL NOT NULL,
    original_price DECIMAL NOT NULL,
    discount_percentage DECIMAL NOT NULL,
    stock_level DECIMAL NOT NULL,
    customer_rating DECIMAL NOT NULL,
    review_count DECIMAL NOT NULL,
    delivery_days DECIMAL NOT NULL,
    seller VARCHAR(255) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    day_of_week INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    sales_quantity_lag_1 DECIMAL,
    sales_quantity_lag_3 DECIMAL,
    sales_quantity_lag_7 DECIMAL,
    price_lag_1 DECIMAL,
    price_lag_3 DECIMAL,
    price_lag_7 DECIMAL,
    sales_quantity_rolling_mean_3 DECIMAL,
    sales_quantity_rolling_mean_7 DECIMAL,
    price_rolling_mean_3 DECIMAL,
    price_rolling_mean_7 DECIMAL,
    price_target DECIMAL,
    sales_target DECIMAL,
    data_type VARCHAR(10) NOT NULL, -- 'train' or 'test'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_name, date, region, data_type)
);

-- Create index on product_name and date
CREATE INDEX IF NOT EXISTS idx_processed_data_product_date ON processed_data(product_name, date);

-- Create index on data_type
CREATE INDEX IF NOT EXISTS idx_processed_data_type ON processed_data(data_type); 