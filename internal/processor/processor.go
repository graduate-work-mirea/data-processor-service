package processor

import (
	"log"

	"github.com/graduate-work-mirea/data-processor-service/internal/db"
	"github.com/graduate-work-mirea/data-processor-service/internal/models"
	"github.com/graduate-work-mirea/data-processor-service/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Processor handles the data processing workflow
type Processor struct {
	db       *db.Database
	rabbitmq *rabbitmq.Client
}

// New creates a new processor
func New(db *db.Database, rabbitmq *rabbitmq.Client) *Processor {
	return &Processor{
		db:       db,
		rabbitmq: rabbitmq,
	}
}

// Start starts the processor
func (p *Processor) Start() error {
	// Consume raw data from RabbitMQ
	msgs, err := p.rabbitmq.ConsumeRawData()
	if err != nil {
		return err
	}

	log.Println("Waiting for messages. To exit press CTRL+C")

	// Process messages
	for msg := range msgs {
		log.Printf("Received a message: %s", msg.Body)

		// Parse raw data
		rawData, err := rabbitmq.ParseRawData(msg.Body)
		if err != nil {
			log.Printf("Error parsing raw data: %v", err)
			msg.Nack(false, false) // Negative acknowledgement, don't requeue
			continue
		}

		// Process raw data
		processedData, err := rawData.Process()
		if err != nil {
			log.Printf("Error processing raw data: %v", err)
			msg.Nack(false, false) // Negative acknowledgement, don't requeue
			continue
		}

		// Save processed data to database
		err = p.db.SaveProcessedDataWithoutRawID(processedData)
		if err != nil {
			log.Printf("Error saving processed data: %v", err)
			msg.Nack(false, true) // Negative acknowledgement, requeue
			continue
		}

		// Publish processed data to RabbitMQ
		err = p.rabbitmq.PublishProcessedData(processedData)
		if err != nil {
			log.Printf("Error publishing processed data: %v", err)
			msg.Nack(false, true) // Negative acknowledgement, requeue
			continue
		}

		// Acknowledge message
		msg.Ack(false)
		log.Printf("Successfully processed data for product %s", processedData.ProductID)
	}

	return nil
}
