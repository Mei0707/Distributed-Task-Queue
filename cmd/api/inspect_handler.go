package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

// InspectHandler exposes read-only endpoints for monitoring queue health.
// This is useful when you don't want to run the full Asynqmon Web UI.
type InspectHandler struct {
	inspector *asynq.Inspector
	logger    *zap.Logger
}

func NewInspectHandler(inspector *asynq.Inspector, logger *zap.Logger) *InspectHandler {
	return &InspectHandler{inspector: inspector, logger: logger}
}

// ListQueues returns all known queues and their sizes.
// GET /api/v1/queues
func (h *InspectHandler) ListQueues(c *gin.Context) {
	queues, err := h.inspector.Queues()
	if err != nil {
		h.logger.Error("Failed to list queues", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"queues": queues})
}

// GetQueueStats returns detailed stats for a specific queue.
// GET /api/v1/queues/:queue/stats
func (h *InspectHandler) GetQueueStats(c *gin.Context) {
	queue := c.Param("queue")
	info, err := h.inspector.GetQueueInfo(queue)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "queue not found: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"queue":   info.Queue,
		"size":    info.Size,
		"active":  info.Active,
		"pending": info.Pending,
		"scheduled": info.Scheduled,
		"retry":   info.Retry,
		"archived": info.Archived, // dead letter queue count
		"processed_today": info.Processed,
		"failed_today":    info.Failed,
		"paused":  info.Paused,
	})
}

// ListActiveTasks returns tasks currently being processed by workers.
// GET /api/v1/queues/:queue/tasks/active
func (h *InspectHandler) ListActiveTasks(c *gin.Context) {
	queue := c.Param("queue")
	page, size := getPagination(c)

	tasks, err := h.inspector.ListActiveTasks(queue,
		asynq.Page(page), asynq.PageSize(size))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "page": page, "page_size": size})
}

// ListPendingTasks returns tasks waiting to be picked up by a worker.
// GET /api/v1/queues/:queue/tasks/pending
func (h *InspectHandler) ListPendingTasks(c *gin.Context) {
	queue := c.Param("queue")
	page, size := getPagination(c)

	tasks, err := h.inspector.ListPendingTasks(queue,
		asynq.Page(page), asynq.PageSize(size))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "page": page, "page_size": size})
}

// ListScheduledTasks returns tasks queued for future processing.
// GET /api/v1/queues/:queue/tasks/scheduled
func (h *InspectHandler) ListScheduledTasks(c *gin.Context) {
	queue := c.Param("queue")
	page, size := getPagination(c)

	tasks, err := h.inspector.ListScheduledTasks(queue,
		asynq.Page(page), asynq.PageSize(size))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "page": page, "page_size": size})
}

// ListRetryTasks returns tasks that failed and are waiting to be retried.
// GET /api/v1/queues/:queue/tasks/retry
func (h *InspectHandler) ListRetryTasks(c *gin.Context) {
	queue := c.Param("queue")
	page, size := getPagination(c)

	tasks, err := h.inspector.ListRetryTasks(queue,
		asynq.Page(page), asynq.PageSize(size))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "page": page, "page_size": size})
}

// ListArchivedTasks returns the dead letter queue — tasks that exhausted all retries.
// These can be manually re-run or deleted via the Asynqmon UI or CLI.
// GET /api/v1/queues/:queue/tasks/archived
func (h *InspectHandler) ListArchivedTasks(c *gin.Context) {
	queue := c.Param("queue")
	page, size := getPagination(c)

	tasks, err := h.inspector.ListArchivedTasks(queue,
		asynq.Page(page), asynq.PageSize(size))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"tasks":    tasks,
		"page":     page,
		"page_size": size,
		"note":     "These tasks have exhausted all retries. Manually re-run or delete via Asynqmon.",
	})
}

// getPagination extracts page and page_size query params with sane defaults.
func getPagination(c *gin.Context) (int, int) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return page, size
}