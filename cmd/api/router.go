package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
	"github.com/yourname/asynq-task-queue/internal/middleware"
	"github.com/yourname/asynq-task-queue/internal/telemetry"
)

// NewRouter creates and configures the Gin HTTP router.
// Routes follow REST conventions: POST creates/enqueues, GET inspects.
func NewRouter(client *asynq.Client, inspector *asynq.Inspector, logger *zap.Logger, store *jobs.Store, cfg *config.Config) *gin.Engine {
	r := gin.New() // Use gin.New() instead of gin.Default() — we add our own middleware

	// --- Global Middleware ---
	r.Use(middleware.Recovery(logger))
	r.Use(middleware.RequestLogger(logger))
	r.Use(telemetry.ObserveHTTP())
	r.GET("/metrics", gin.WrapH(telemetry.Handler()))
	r.GET("/ready", func(c *gin.Context) {
		if store == nil || store.Pool.Ping(c.Request.Context()) != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		if _, err := inspector.Queues(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	if err := registerDashboardRoutes(r); err != nil {
		panic(fmt.Sprintf("failed to register dashboard routes: %v", err))
	}

	// --- Health Check ---
	// Used by load balancers and Kubernetes liveness probes
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// --- Task Enqueueing Endpoints ---
	submitter := &SubmissionHandler{store: store, worker: cfg.Worker, logger: logger}
	emailHandler := NewEmailHandler(submitter, logger)
	imageHandler := NewImageHandler(submitter, logger)
	inspectHandler := NewInspectHandler(inspector, logger, store)

	v1 := r.Group("/api/v1")
	{
		v1.GET("/jobs", submitter.List)
		v1.GET("/jobs/:id", submitter.Get)
		v1.POST("/jobs/:id/cancel", submitter.Cancel)
		tasks := v1.Group("/tasks")
		{
			// Email tasks
			tasks.POST("/email", emailHandler.Enqueue)

			// Image processing tasks
			tasks.POST("/image/resize", imageHandler.EnqueueResize)
		}

		// Queue inspection endpoints (read-only)
		queues := v1.Group("/queues")
		{
			queues.GET("", inspectHandler.ListQueues)
			queues.GET("/:queue/stats", inspectHandler.GetQueueStats)
			queues.POST("/:queue/pause", inspectHandler.PauseQueue)
			queues.POST("/:queue/unpause", inspectHandler.UnpauseQueue)
			queues.GET("/:queue/tasks/active", inspectHandler.ListActiveTasks)
			queues.GET("/:queue/tasks/pending", inspectHandler.ListPendingTasks)
			queues.GET("/:queue/tasks/scheduled", inspectHandler.ListScheduledTasks)
			queues.GET("/:queue/tasks/retry", inspectHandler.ListRetryTasks)
			queues.GET("/:queue/tasks/archived", inspectHandler.ListArchivedTasks) // dead letter queue
			queues.POST("/:queue/tasks/:task_id/run", inspectHandler.RunTask)
			queues.POST("/:queue/tasks/:task_id/archive", inspectHandler.ArchiveTask)
			queues.DELETE("/:queue/tasks/:task_id", inspectHandler.DeleteTask)
		}
	}

	return r
}
