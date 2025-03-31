package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/graduate-work-mirea/data-processor-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// RawDataQueue is the name of the queue for raw data
	RawDataQueue = "raw_data_queue"
	// ProcessedDataQueue is the name of the queue for processed data
	ProcessedDataQueue = "processed_data_queue"
)

// Client represents a RabbitMQ client
type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// New creates a new RabbitMQ client
func New(url string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Declare queues
	err = declareQueues(ch)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &Client{
		conn:    conn,
		channel: ch,
	}, nil
}

// declareQueues declares the necessary queues
func declareQueues(ch *amqp.Channel) error {
	// Declare raw data queue
	_, err := ch.QueueDeclare(
		RawDataQueue, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare raw data queue: %w", err)
	}

	// Declare processed data queue
	_, err = ch.QueueDeclare(
		ProcessedDataQueue, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare processed data queue: %w", err)
	}

	return nil
}

// Close closes the RabbitMQ connection and channel
func (c *Client) Close() error {
	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %w", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	return nil
}

// ConsumeRawData consumes raw data from the queue
func (c *Client) ConsumeRawData() (<-chan amqp.Delivery, error) {
	return c.channel.Consume(
		RawDataQueue, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
}

// PublishProcessedData publishes processed data to the queue
func (c *Client) PublishProcessedData(data *models.ProcessedData) error {
	jsonData, err := data.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert processed data to JSON: %w", err)
	}

	ctx := context.Background()
	err = c.channel.PublishWithContext(
		ctx,
		"",                 // exchange
		ProcessedDataQueue, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(jsonData),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish processed data: %w", err)
	}

	log.Printf("Published processed data for product %s", data.ProductID)
	return nil
}

// ParseRawData parses raw data from a RabbitMQ message
func ParseRawData(msg []byte) (*models.RawData, error) {
	var rawData models.RawData
	if err := json.Unmarshal(msg, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw data: %w", err)
	}
	return &rawData, nil
}
