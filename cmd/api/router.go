package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/middleware"
)

// NewRouter creates and configures the Gin HTTP router.
// Routes follow REST conventions: POST creates/enqueues, GET inspects.
func NewRouter(client *asynq.Client, inspector *asynq.Inspector, logger *zap.Logger) *gin.Engine {
	r := gin.New() // Use gin.New() instead of gin.Default() — we add our own middleware

	// --- Global Middleware ---
	r.Use(middleware.Recovery(logger))
	r.Use(middleware.RequestLogger(logger))

	if err := registerDashboardRoutes(r); err != nil {
		panic(fmt.Sprintf("failed to register dashboard routes: %v", err))
	}

	// --- Health Check ---
	// Used by load balancers and Kubernetes liveness probes
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// --- Task Enqueueing Endpoints ---
	emailHandler := NewEmailHandler(client, logger)
	imageHandler := NewImageHandler(client, logger)
	inspectHandler := NewInspectHandler(inspector, logger)

	v1 := r.Group("/api/v1")
	{
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
			queues.GET("/:queue/tasks/active", inspectHandler.ListActiveTasks)
			queues.GET("/:queue/tasks/pending", inspectHandler.ListPendingTasks)
			queues.GET("/:queue/tasks/scheduled", inspectHandler.ListScheduledTasks)
			queues.GET("/:queue/tasks/retry", inspectHandler.ListRetryTasks)
			queues.GET("/:queue/tasks/archived", inspectHandler.ListArchivedTasks) // dead letter queue
		}
	}

	return r
}
