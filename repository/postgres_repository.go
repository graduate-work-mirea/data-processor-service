package repository

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// PostgresRepository handles interactions with PostgreSQL
type PostgresRepository struct {
	pool   *pgxpool.Pool
	logger *zap.SugaredLogger
}

// NewPostgresRepository creates a new PostgresRepository
func NewPostgresRepository(connString string, logger *zap.SugaredLogger) (*PostgresRepository, error) {
	// Create connection pool
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %v", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %v", err)
	}

	repo := &PostgresRepository{
		pool:   pool,
		logger: logger,
	}

	return repo, nil
}

// RunMigrations runs database migrations
func (r *PostgresRepository) RunMigrations(dbName string) error {
	// Extract connection details from pgx pool to create a sql.DB connection
	config, err := pgxpool.ParseConfig(r.pool.Config().ConnString())
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %v", err)
	}

	// Create a sql.DB connection string
	sqlConnStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.ConnConfig.User,
		config.ConnConfig.Password,
		config.ConnConfig.Host,
		config.ConnConfig.Port,
		config.ConnConfig.Database,
		"disable", // Use your desired SSL mode
	)

	// Open a database connection specifically for migrations
	db, err := sql.Open("postgres", sqlConnStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create migration driver
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %v", err)
	}

	// Create migration instance
	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		dbName,
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %v", err)
	}

	// Run migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %v", err)
	}

	r.logger.Info("Database migrations completed successfully")
	return nil
}

// Close closes the database connection pool
func (r *PostgresRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// SaveProcessedData saves processed data to the database
func (r *PostgresRepository) SaveProcessedData(filePath string, dataType string) error {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	// Create a map of column indices
	colIndices := make(map[string]int)
	for i, colName := range header {
		colIndices[colName] = i
	}

	// Prepare SQL statement
	sql := `
		INSERT INTO processed_data (
			product_name, date, region, brand, category, 
			sales_quantity, price, original_price, discount_percentage, 
			stock_level, customer_rating, review_count, delivery_days, 
			seller, is_weekend, is_holiday, day_of_week, month, quarter,
			sales_quantity_lag_1, sales_quantity_lag_3, sales_quantity_lag_7, 
			price_lag_1, price_lag_3, price_lag_7, 
			sales_quantity_rolling_mean_3, sales_quantity_rolling_mean_7,
			price_rolling_mean_3, price_rolling_mean_7,
			price_target, sales_target, data_type
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32
		) ON CONFLICT (product_name, date, region, data_type) DO UPDATE SET
			brand = EXCLUDED.brand,
			category = EXCLUDED.category,
			sales_quantity = EXCLUDED.sales_quantity,
			price = EXCLUDED.price,
			original_price = EXCLUDED.original_price,
			discount_percentage = EXCLUDED.discount_percentage,
			stock_level = EXCLUDED.stock_level,
			customer_rating = EXCLUDED.customer_rating,
			review_count = EXCLUDED.review_count,
			delivery_days = EXCLUDED.delivery_days,
			seller = EXCLUDED.seller,
			is_weekend = EXCLUDED.is_weekend,
			is_holiday = EXCLUDED.is_holiday,
			day_of_week = EXCLUDED.day_of_week,
			month = EXCLUDED.month,
			quarter = EXCLUDED.quarter,
			sales_quantity_lag_1 = EXCLUDED.sales_quantity_lag_1,
			sales_quantity_lag_3 = EXCLUDED.sales_quantity_lag_3,
			sales_quantity_lag_7 = EXCLUDED.sales_quantity_lag_7,
			price_lag_1 = EXCLUDED.price_lag_1,
			price_lag_3 = EXCLUDED.price_lag_3,
			price_lag_7 = EXCLUDED.price_lag_7,
			sales_quantity_rolling_mean_3 = EXCLUDED.sales_quantity_rolling_mean_3,
			sales_quantity_rolling_mean_7 = EXCLUDED.sales_quantity_rolling_mean_7,
			price_rolling_mean_3 = EXCLUDED.price_rolling_mean_3,
			price_rolling_mean_7 = EXCLUDED.price_rolling_mean_7,
			price_target = EXCLUDED.price_target,
			sales_target = EXCLUDED.sales_target
	`

	// Start a transaction
	tx, err := r.pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

	// Use a batch for more efficient inserts
	batch := &pgx.Batch{}
	count := 0

	// Read the rest of the rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading row: %v", err)
		}

		// Extract and convert values (handling nulls as needed)
		var params []interface{}
		params = append(params, row[colIndices["product_name"]])                      // product_name
		params = append(params, row[colIndices["date"]])                              // date
		params = append(params, row[colIndices["region"]])                            // region
		params = append(params, row[colIndices["brand"]])                             // brand
		params = append(params, row[colIndices["category"]])                          // category
		params = append(params, parseDecimal(row[colIndices["sales_quantity"]]))      // sales_quantity
		params = append(params, parseDecimal(row[colIndices["price"]]))               // price
		params = append(params, parseDecimal(row[colIndices["original_price"]]))      // original_price
		params = append(params, parseDecimal(row[colIndices["discount_percentage"]])) // discount_percentage
		params = append(params, parseDecimal(row[colIndices["stock_level"]]))         // stock_level
		params = append(params, parseDecimal(row[colIndices["customer_rating"]]))     // customer_rating
		params = append(params, parseDecimal(row[colIndices["review_count"]]))        // review_count
		params = append(params, parseDecimal(row[colIndices["delivery_days"]]))       // delivery_days
		params = append(params, row[colIndices["seller"]])                            // seller
		params = append(params, row[colIndices["is_weekend"]] == "True")              // is_weekend
		params = append(params, row[colIndices["is_holiday"]] == "True")              // is_holiday
		params = append(params, parseInt(row[colIndices["day_of_week"]]))             // day_of_week
		params = append(params, parseInt(row[colIndices["month"]]))                   // month
		params = append(params, parseInt(row[colIndices["quarter"]]))                 // quarter

		// Optional fields with lag data
		params = append(params, parseNullableDecimal(row, colIndices, "sales_quantity_lag_1"))
		params = append(params, parseNullableDecimal(row, colIndices, "sales_quantity_lag_3"))
		params = append(params, parseNullableDecimal(row, colIndices, "sales_quantity_lag_7"))
		params = append(params, parseNullableDecimal(row, colIndices, "price_lag_1"))
		params = append(params, parseNullableDecimal(row, colIndices, "price_lag_3"))
		params = append(params, parseNullableDecimal(row, colIndices, "price_lag_7"))

		// Optional fields with rolling means
		params = append(params, parseNullableDecimal(row, colIndices, "sales_quantity_rolling_mean_3"))
		params = append(params, parseNullableDecimal(row, colIndices, "sales_quantity_rolling_mean_7"))
		params = append(params, parseNullableDecimal(row, colIndices, "price_rolling_mean_3"))
		params = append(params, parseNullableDecimal(row, colIndices, "price_rolling_mean_7"))

		// Target fields
		params = append(params, parseDecimal(row[colIndices["price_target"]])) // price_target
		params = append(params, parseDecimal(row[colIndices["sales_target"]])) // sales_target
		params = append(params, dataType)                                      // data_type

		// Add query to batch
		batch.Queue(sql, params...)
		count++

		// Execute batch every 1000 rows
		if count%1000 == 0 {
			br := r.pool.SendBatch(context.Background(), batch)
			_, err = br.Exec()
			if err != nil {
				return fmt.Errorf("error executing batch: %v", err)
			}
			err = br.Close()
			if err != nil {
				return fmt.Errorf("error closing batch results: %v", err)
			}
			batch = &pgx.Batch{}
			r.logger.Infof("Inserted %d rows", count)
		}
	}

	// Execute any remaining batch items
	if count%1000 != 0 {
		br := r.pool.SendBatch(context.Background(), batch)
		_, err = br.Exec()
		if err != nil {
			return fmt.Errorf("error executing final batch: %v", err)
		}
		err = br.Close()
		if err != nil {
			return fmt.Errorf("error closing final batch results: %v", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	r.logger.Infof("Successfully saved %d rows of %s data to the database", count, dataType)
	return nil
}

// Helper functions for type conversion

func parseDecimal(val string) float64 {
	if val == "" || val == "NaN" {
		return 0
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0
	}
	return f
}

func parseNullableDecimal(row []string, colIndices map[string]int, colName string) interface{} {
	idx, exists := colIndices[colName]
	if !exists || idx >= len(row) {
		return nil
	}
	val := strings.TrimSpace(row[idx])
	if val == "" || val == "NaN" || val == "nan" || val == "None" {
		return nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil
	}
	return f
}

func parseInt(val string) int {
	if val == "" {
		return 0
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return i
}
