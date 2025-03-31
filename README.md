# Data Processor Service

A microservice for processing raw data for a product demand evaluation system. This service:

- Consumes raw data from RabbitMQ
- Normalizes and validates the data
- Stores processed data in PostgreSQL
- Forwards processed data to another microservice via RabbitMQ

## How It Works

### Input
- Service consumes raw data from RabbitMQ queue `raw_data_queue`
- Data format: JSON with fields:
  ```json
  {
    "product_id": "123",
    "sales": 100,
    "date": "2024-10-01"
  }
  ```

### Processing
1. **Data Validation**: Checks for required fields (product_id, sales, date)
2. **Data Normalization**:
   - Converts `sales` to integer
   - Formats `date` to ISO 8601 format (YYYY-MM-DDT00:00:00Z)
3. **Error Handling**: 
   - Logs and skips invalid records
   - Doesn't attempt to fix or fill missing data (MVP approach)

### Output
1. **Database Storage**:
   - Saves processed data to PostgreSQL in `processed_data` table
   - Stores in JSON format with normalized values
2. **Message Publishing**:
   - Forwards processed data to RabbitMQ queue `processed_data_queue`
   - Output format:
     ```json
     {
       "product_id": "123",
       "sales": 100,
       "date": "2024-10-01T00:00:00Z"
     }
     ```

## Features

- Basic data normalization and validation
- Persistent storage in PostgreSQL
- Asynchronous communication via RabbitMQ
- Containerized with Docker

## Requirements

- Go 1.21+
- RabbitMQ
- PostgreSQL

## Environment Variables

- `RABBITMQ_URL`: Connection URL for RabbitMQ
- `POSTGRES_DSN`: Connection string for PostgreSQL

## Running the Service

### Using Docker

```bash
docker build -t data-processor-service .
docker run -e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/ -e POSTGRES_DSN="host=postgres user=postgres password=postgres dbname=demanddb sslmode=disable" data-processor-service
```

### Using Docker Compose

```bash
docker-compose up -d
```

### Locally

```bash
go run cmd/main.go