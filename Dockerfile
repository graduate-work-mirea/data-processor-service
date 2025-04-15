FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o data-processor-service .

# Create final image with Python and Go binary
FROM python:3.10-slim

WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
RUN pip install --no-cache-dir pandas numpy scikit-learn pyarrow psycopg2-binary

# Copy the Go binary from builder stage
COPY --from=builder /app/data-processor-service .

# Copy Python scripts
COPY scripts/ ./scripts/

# Copy migrations
COPY migrations/ ./migrations/

# Create data directories
RUN mkdir -p /app/data/raw /app/data/processed

# Create .env file from example if needed
COPY --from=builder /app/.env.example ./.env

# Run the application
CMD ["./data-processor-service"]
