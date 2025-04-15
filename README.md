# Data Processor Service

This service processes marketplace data for machine learning predictions. It consumes data from a RabbitMQ queue, processes it using a Python script, and saves the processed data for use by the ML service.

## Features

- Consumes marketplace data from RabbitMQ
- Processes data using Python for feature engineering
- Creates lag features and rolling statistics
- Normalizes numerical features
- Generates target variables for price and sales prediction
- Splits data into training and testing sets
- Saves processed data in CSV format
- Stores processed data in PostgreSQL database

## Architecture

The service follows a layered architecture:

- **Controller**: Handles incoming data from RabbitMQ
- **Service**: Contains business logic for data processing
- **Repository**: Manages file operations, database storage and RabbitMQ interactions
- **Assembly**: Wires up all components

## Data Processing Pipeline

1. **Data Loading**: Consumes data from RabbitMQ queue
2. **Basic Processing**:
   - Converts date to datetime format
   - Converts numeric and boolean fields to appropriate types
   - Handles missing values
   - Removes duplicates
3. **Feature Engineering**:
   - Extracts time features (day_of_week, month, quarter)
   - Creates lag features for sales and price
   - Calculates rolling statistics
   - Creates target variables for prediction
4. **Data Splitting**:
   - Splits data into training and testing sets based on date
5. **Data Saving**:
   - Saves processed data in CSV format
   - Stores processed data in PostgreSQL database

## Configuration

The service can be configured using environment variables:

- `RABBITMQ_URL`: RabbitMQ connection URL (default: "amqp://guest:guest@localhost:5672/")
- `DATA_QUEUE_NAME`: RabbitMQ queue name (default: "marketplace_data")
- `SCHEDULER_INTERVAL_HOURS`: Interval for processing data in hours (default: 24)
- `DATA_PATH`: Path for storing data (default: "./data")
- `SCRIPTS_PATH`: Path to Python scripts (default: "./scripts")
- `PYTHON_PATH`: Path to Python executable (default: "python")
- `CUTOFF_DATE`: Date for train/test split (default: "2025-03-20")
- `BATCH_SIZE`: Number of messages to consume in one batch (default: 1000)
- `CONSUME_TIMEOUT_SECONDS`: Timeout for consuming messages (default: 60)
- `POSTGRES_HOST`: PostgreSQL host (default: "localhost")
- `POSTGRES_PORT`: PostgreSQL port (default: "5432")
- `POSTGRES_USER`: PostgreSQL user (default: "postgres")
- `POSTGRES_PASSWORD`: PostgreSQL password (default: "postgres")
- `POSTGRES_DB_NAME`: PostgreSQL database name (default: "marketplace_data")
- `POSTGRES_SSL_MODE`: PostgreSQL SSL mode (default: "disable")

## Setup and Running

### Prerequisites

- Go 1.16+
- Python 3.8+
- RabbitMQ server
- PostgreSQL 12+

### Python Dependencies

Install the required Python packages:

```bash
pip install pandas numpy scikit-learn psycopg2-binary
```

### Running with Docker Compose

The easiest way to run the service with all dependencies is using Docker Compose:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

### Running the Service Locally

1. Clone the repository
2. Set up environment variables (or create a `.env` file)
3. Build and run the service:

```bash
go build
./data-processor-service
```

## PostgreSQL Database Structure

The service automatically creates and maintains a PostgreSQL database table:

```sql
CREATE TABLE processed_data (
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Input Data Format

The service expects data in the following JSON format:

```json
{
  "product_name": "Футболка Zara базовая хлопковая",
  "brand": "Zara",
  "date": "2025-03-29",
  "sales_quantity": 14,
  "price": 2300,
  "original_price": 0,
  "discount_percentage": 0,
  "stock_level": 304,
  "region": "Уфа",
  "category": "Одежда",
  "customer_rating": 4.4,
  "review_count": 221,
  "delivery_days": 3,
  "seller": "ОАО «Кондратьев, Марков и Кудрявцев»",
  "is_weekend": true,
  "is_holiday": false
}
```

## Output Data Format

The processed data includes the following columns:

- `product_name`: Product name
- `date`: Date in datetime format
- `day_of_week`: Day of week (0-6, where 0 is Monday)
- `month`: Month (1-12)
- `quarter`: Quarter (1-4)
- `is_weekend`: Whether the date is a weekend
- `is_holiday`: Whether the date is a holiday
- `sales_quantity_lag_*`: Sales quantity lag features
- `price_lag_*`: Price lag features
- `sales_quantity_rolling_mean_*`: Sales quantity rolling mean features
- `price_rolling_mean_*`: Price rolling mean features
- `price`, `original_price`, `discount_percentage`, `stock_level`, `customer_rating`, `review_count`, `delivery_days`: Numeric features
- `brand`, `region`, `category`, `seller`: Categorical features
- `price_target`: Price after 7 days
- `sales_target`: Sum of sales for the next 7 days
- `data_type`: Type of data ("train" or "test")