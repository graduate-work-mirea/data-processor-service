package controller

import (
	"context"
	"time"

	"github.com/graduate-work-mirea/data-processor-service/service"
	"go.uber.org/zap"
)

// RabbitMQController handles incoming data from RabbitMQ
type RabbitMQController struct {
	dataProcessorService *service.DataProcessorService
	logger               *zap.SugaredLogger
}

// NewRabbitMQController creates a new RabbitMQController instance
func NewRabbitMQController(dataProcessorService *service.DataProcessorService, logger *zap.SugaredLogger) *RabbitMQController {
	return &RabbitMQController{
		dataProcessorService: dataProcessorService,
		logger:               logger,
	}
}

// StartProcessing starts processing data from RabbitMQ
func (c *RabbitMQController) StartProcessing(ctx context.Context, interval time.Duration) {
	c.logger.Info("Starting RabbitMQ controller")

	// Start a goroutine to periodically process data
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Process data immediately on start
		if err := c.dataProcessorService.ProcessMarketplaceData(ctx); err != nil {
			c.logger.Errorf("Failed to process marketplace data: %v", err)
		}

		for {
			select {
			case <-ctx.Done():
				c.logger.Info("RabbitMQ controller stopped")
				return
			case <-ticker.C:
				c.logger.Info("Processing marketplace data (scheduled)")
				if err := c.dataProcessorService.ProcessMarketplaceData(ctx); err != nil {
					c.logger.Errorf("Failed to process marketplace data: %v", err)
				}
			}
		}
	}()
}
