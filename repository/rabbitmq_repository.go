package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/graduate-work-mirea/data-processor-service/internal/rabbitmq"
	"go.uber.org/zap"
)

// RabbitMQRepository handles RabbitMQ operations
type RabbitMQRepository struct {
	client    *rabbitmq.Client
	queueName string
	logger    *zap.SugaredLogger
}

// NewRabbitMQRepository creates a new RabbitMQRepository instance
func NewRabbitMQRepository(client *rabbitmq.Client, queueName string, logger *zap.SugaredLogger) *RabbitMQRepository {
	return &RabbitMQRepository{
		client:    client,
		queueName: queueName,
		logger:    logger,
	}
}

// ConsumeMessages consumes messages from the RabbitMQ queue
func (r *RabbitMQRepository) ConsumeMessages(ctx context.Context, batchSize int, timeout time.Duration) ([]map[string]interface{}, error) {
	r.logger.Infof("Starting to consume messages from queue: %s", r.queueName)

	// Declare queue
	q, err := r.client.DeclareQueue(r.queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Set QoS for batch processing
	err = r.client.Channel().Qos(
		batchSize, // prefetch count
		0,         // prefetch size
		false,     // global
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	// Consume messages
	msgs, err := r.client.Channel().Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	var data []map[string]interface{}
	count := 0
	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping message consumption")
			return data, nil
		case <-timeoutCh:
			r.logger.Infof("Timeout reached after consuming %d messages", count)
			return data, nil
		case msg, ok := <-msgs:
			if !ok {
				r.logger.Info("Channel closed, stopping message consumption")
				return data, nil
			}

			// Parse message
			var item map[string]interface{}
			if err := json.Unmarshal(msg.Body, &item); err != nil {
				r.logger.Warnf("Failed to unmarshal message: %v", err)
				msg.Nack(false, false) // Reject message without requeue
				continue
			}

			// Add to batch
			data = append(data, item)
			count++

			// Acknowledge message
			if err := msg.Ack(false); err != nil {
				r.logger.Warnf("Failed to acknowledge message: %v", err)
			}

			// Check if we've reached the batch size
			if count >= batchSize {
				r.logger.Infof("Batch size reached, consumed %d messages", count)
				return data, nil
			}
		}
	}
}

// PublishProcessedData publishes processed data to a queue
func (r *RabbitMQRepository) PublishProcessedData(ctx context.Context, queueName string, data interface{}) error {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Publish message
	err = r.client.PublishMessage(ctx, queueName, jsonData)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
