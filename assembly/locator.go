package assembly

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/graduate-work-mirea/data-processor-service/config"
	"github.com/graduate-work-mirea/data-processor-service/controller"
	"github.com/graduate-work-mirea/data-processor-service/internal/rabbitmq"
	"github.com/graduate-work-mirea/data-processor-service/repository"
	"github.com/graduate-work-mirea/data-processor-service/service"
	"go.uber.org/zap"
)

type ServiceLocator struct {
	Config               *config.Config
	RabbitClient         *rabbitmq.Client
	Logger               *zap.SugaredLogger
	FileRepository       *repository.FileRepository
	RabbitMQRepository   *repository.RabbitMQRepository
	PostgresRepository   *repository.PostgresRepository
	DataProcessorService *service.DataProcessorService
	RabbitMQController   *controller.RabbitMQController
}

func NewServiceLocator(cfg *config.Config, logger *zap.SugaredLogger) (*ServiceLocator, error) {
	// Initialize RabbitMQ client
	rabbitClient, err := rabbitmq.NewClient(cfg.RabbitMQURL, logger)
	if err != nil {
		return nil, err
	}

	// Declare queue
	_, err = rabbitClient.DeclareQueue(cfg.DataQueueName)
	if err != nil {
		rabbitClient.Close()
		return nil, err
	}

	// Initialize PostgreSQL connection
	connString := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.PostgresHost, cfg.PostgresPort, cfg.PostgresUser,
		cfg.PostgresPassword, cfg.PostgresDBName, cfg.PostgresSSLMode,
	)

	var postgresRepo *repository.PostgresRepository
	postgresRepo, err = repository.NewPostgresRepository(connString, logger)
	if err != nil {
		logger.Warnf("Failed to initialize PostgreSQL repository: %v", err)
		logger.Warn("Continuing without PostgreSQL connection, data will only be saved to files")
		postgresRepo = nil
	} else {
		// Run migrations only if connection was successful
		if err := postgresRepo.RunMigrations(cfg.PostgresDBName); err != nil {
			logger.Warnf("Failed to run migrations: %v", err)
			postgresRepo.Close()
			postgresRepo = nil
			logger.Warn("Continuing without PostgreSQL connection, data will only be saved to files")
		} else {
			logger.Info("PostgreSQL connection and migrations successful")
		}
	}

	// Initialize repositories
	fileRepo := repository.NewFileRepository(cfg.DataPath)
	rabbitRepo := repository.NewRabbitMQRepository(rabbitClient, cfg.DataQueueName, logger)

	// Initialize service
	scriptPath := filepath.Join(cfg.ScriptsPath, "data_processor.py")
	dataProcessorService := service.NewDataProcessorService(
		fileRepo,
		rabbitRepo,
		postgresRepo,
		cfg.PythonPath,
		scriptPath,
		cfg.CutoffDate,
		cfg.BatchSize,
		time.Duration(cfg.ConsumeTimeoutSeconds)*time.Second,
		logger,
	)

	// Initialize controller
	rabbitMQController := controller.NewRabbitMQController(dataProcessorService, logger)

	return &ServiceLocator{
		Config:               cfg,
		RabbitClient:         rabbitClient,
		Logger:               logger,
		FileRepository:       fileRepo,
		RabbitMQRepository:   rabbitRepo,
		PostgresRepository:   postgresRepo,
		DataProcessorService: dataProcessorService,
		RabbitMQController:   rabbitMQController,
	}, nil
}

func (l *ServiceLocator) Close() {
	if l.RabbitClient != nil {
		l.RabbitClient.Close()
	}
	if l.PostgresRepository != nil {
		l.PostgresRepository.Close()
	}
}
