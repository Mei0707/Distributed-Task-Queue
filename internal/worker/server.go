package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
	"github.com/yourname/asynq-task-queue/internal/tasks/email"
	"github.com/yourname/asynq-task-queue/internal/tasks/image"
)

// Server wraps the asynq.Server and owns all task handler registrations.
type Server struct {
	asynqServer *asynq.Server
	mux         *asynq.ServeMux
	logger      *zap.Logger
}

// New creates a fully configured worker server.
func New(cfg *config.Config, logger *zap.Logger, store *jobs.Store) (*Server, error) {
	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	}

	asynqCfg := asynq.Config{
		Concurrency:     cfg.Worker.Concurrency,
		ShutdownTimeout: 30 * time.Second,
		// Weighted priority: workers pick from queues proportionally to weights.
		// critical=6 gets ~60%, default=3 gets ~30%, low=1 gets ~10%.
		Queues: map[string]int{
			"critical": cfg.Queue.Critical,
			"default":  cfg.Queue.Default,
			"low":      cfg.Queue.Low,
		},
		// Asynq invokes this on every failed execution, including transient errors.
		ErrorHandler: asynq.ErrorHandlerFunc(
			func(ctx context.Context, task *asynq.Task, err error) {
				logger.Error("Task execution failed",
					zap.String("task_type", task.Type()),
					zap.Error(err),
				)
			},
		),
		Logger: newAsynqLogger(logger),
	}

	srv := asynq.NewServer(redisOpt, asynqCfg)

	mux := asynq.NewServeMux()
	mux.Use(store.Middleware())
	mux.Use(taskLoggingMiddleware(logger))
	mux.Handle(email.TypeEmailDelivery, email.NewHandler(logger))
	mux.Handle(image.TypeImageResize, image.NewHandler(logger))

	return &Server{asynqServer: srv, mux: mux, logger: logger}, nil
}

// Start begins processing tasks and returns after startup. The caller owns signals.
func (s *Server) Start() error {
	s.logger.Info("Starting task worker",
		zap.String("handlers", fmt.Sprintf("%s, %s",
			email.TypeEmailDelivery, image.TypeImageResize)),
	)
	return s.asynqServer.Start(s.mux)
}

// Shutdown gracefully stops — in-flight tasks finish before process exits.
func (s *Server) Shutdown() {
	s.logger.Info("Shutting down worker — waiting for in-flight tasks...")
	s.asynqServer.Shutdown()
	s.logger.Info("Worker shutdown complete")
}

// taskLoggingMiddleware logs the start and completion of every task.
func taskLoggingMiddleware(logger *zap.Logger) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			logger.Info("Task started", zap.String("type", task.Type()))
			err := next.ProcessTask(ctx, task)
			if err != nil {
				logger.Warn("Task failed — will retry if retries remain",
					zap.String("type", task.Type()),
					zap.Error(err),
				)
			} else {
				logger.Info("Task completed successfully", zap.String("type", task.Type()))
			}
			return err
		})
	}
}

// asynqLogger adapts zap to the asynq.Logger interface.
type asynqLogger struct{ logger *zap.Logger }

func newAsynqLogger(l *zap.Logger) *asynqLogger {
	return &asynqLogger{logger: l.WithOptions(zap.AddCallerSkip(1))}
}
func (l *asynqLogger) Debug(args ...interface{}) { l.logger.Sugar().Debug(args...) }
func (l *asynqLogger) Info(args ...interface{})  { l.logger.Sugar().Info(args...) }
func (l *asynqLogger) Warn(args ...interface{})  { l.logger.Sugar().Warn(args...) }
func (l *asynqLogger) Error(args ...interface{}) { l.logger.Sugar().Error(args...) }
func (l *asynqLogger) Fatal(args ...interface{}) { l.logger.Sugar().Fatal(args...) }
