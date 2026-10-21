package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/jobs"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

// InspectHandler exposes queue monitoring and transport-control endpoints.
// This is useful when you don't want to run the full Asynqmon Web UI.
type InspectHandler struct {
	inspector *asynq.Inspector
	logger    *zap.Logger
	store     *jobs.Store
}

func NewInspectHandler(inspector *asynq.Inspector, logger *zap.Logger, store *jobs.Store) *InspectHandler {
	return &InspectHandler{inspector: inspector, logger: logger, store: store}
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
		"queue":           info.Queue,
		"size":            info.Size,
		"active":          info.Active,
		"pending":         info.Pending,
		"scheduled":       info.Scheduled,
		"retry":           info.Retry,
		"archived":        info.Archived, // dead letter queue count
		"processed_today": info.Processed,
		"failed_today":    info.Failed,
		"paused":          info.Paused,
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
	c.JSON(http.StatusOK, gin.H{"tasks": taskViews(tasks), "page": page, "page_size": size})
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
	c.JSON(http.StatusOK, gin.H{"tasks": taskViews(tasks), "page": page, "page_size": size})
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
	c.JSON(http.StatusOK, gin.H{"tasks": taskViews(tasks), "page": page, "page_size": size})
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
	c.JSON(http.StatusOK, gin.H{"tasks": taskViews(tasks), "page": page, "page_size": size})
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
		"tasks":     tasks,
		"page":      page,
		"page_size": size,
		"note":      "These tasks have exhausted all retries. Manually re-run or delete via Asynqmon.",
	})
}

// PauseQueue stops workers from pulling new tasks from the queue.
// POST /api/v1/queues/:queue/pause
func (h *InspectHandler) PauseQueue(c *gin.Context) {
	queue := c.Param("queue")
	if err := h.inspector.PauseQueue(queue); err != nil {
		h.logger.Error("Failed to pause queue", zap.String("queue", queue), zap.Error(err))
		response.BadRequest(c, "Failed to pause queue: "+err.Error())
		return
	}

	response.OK(c, gin.H{
		"queue":  queue,
		"action": "paused",
	})
}

// UnpauseQueue resumes workers pulling tasks from the queue.
// POST /api/v1/queues/:queue/unpause
func (h *InspectHandler) UnpauseQueue(c *gin.Context) {
	queue := c.Param("queue")
	if err := h.inspector.UnpauseQueue(queue); err != nil {
		h.logger.Error("Failed to unpause queue", zap.String("queue", queue), zap.Error(err))
		response.BadRequest(c, "Failed to unpause queue: "+err.Error())
		return
	}

	response.OK(c, gin.H{
		"queue":  queue,
		"action": "unpaused",
	})
}

// RunTask moves a scheduled, retry, or archived task back to pending.
// POST /api/v1/queues/:queue/tasks/:task_id/run
func (h *InspectHandler) RunTask(c *gin.Context) {
	queue := c.Param("queue")
	taskID := c.Param("task_id")

	if err := h.inspector.RunTask(queue, taskID); err != nil {
		h.logger.Error("Failed to run task", zap.String("queue", queue), zap.String("task_id", taskID), zap.Error(err))
		response.BadRequest(c, "Failed to run task: "+err.Error())
		return
	}

	response.OK(c, gin.H{
		"queue":   queue,
		"task_id": taskID,
		"action":  "run",
	})
}

// ArchiveTask moves a pending, scheduled, or retry task to archived.
// POST /api/v1/queues/:queue/tasks/:task_id/archive
func (h *InspectHandler) ArchiveTask(c *gin.Context) {
	if h.durableTask(c) {
		return
	}
	queue := c.Param("queue")
	taskID := c.Param("task_id")

	if err := h.inspector.ArchiveTask(queue, taskID); err != nil {
		h.logger.Error("Failed to archive task", zap.String("queue", queue), zap.String("task_id", taskID), zap.Error(err))
		response.BadRequest(c, "Failed to archive task: "+err.Error())
		return
	}

	response.OK(c, gin.H{
		"queue":   queue,
		"task_id": taskID,
		"action":  "archived",
	})
}

// DeleteTask removes a pending, scheduled, retry, or archived task.
// DELETE /api/v1/queues/:queue/tasks/:task_id
func (h *InspectHandler) DeleteTask(c *gin.Context) {
	if h.durableTask(c) {
		return
	}
	queue := c.Param("queue")
	taskID := c.Param("task_id")

	if err := h.inspector.DeleteTask(queue, taskID); err != nil {
		h.logger.Error("Failed to delete task", zap.String("queue", queue), zap.String("task_id", taskID), zap.Error(err))
		response.BadRequest(c, "Failed to delete task: "+err.Error())
		return
	}

	response.OK(c, gin.H{
		"queue":   queue,
		"task_id": taskID,
		"action":  "deleted",
	})
}

// Direct Redis deletion could strand an accepted database job. Cancellation is
// serialized with execution in PostgreSQL; the transport message can then drain.
func (h *InspectHandler) durableTask(c *gin.Context) bool {
	if h.store == nil {
		return false
	}
	_, err := h.store.Get(c.Request.Context(), c.Param("task_id"))
	if errors.Is(err, jobs.ErrNotFound) {
		return false
	}
	if err != nil {
		response.InternalError(c, "Failed to check durable job")
		return true
	}
	c.JSON(http.StatusConflict, gin.H{"error": "Use POST /api/v1/jobs/" + c.Param("task_id") + "/cancel to cancel a durable job"})
	return true
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

func taskViews(tasks []*asynq.TaskInfo) []gin.H {
	result := make([]gin.H, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, gin.H{"id": task.ID, "type": task.Type, "state": task.State.String(), "payload": json.RawMessage(task.Payload), "retry_count": task.Retried, "last_error": task.LastErr, "next_process_at": task.NextProcessAt})
	}
	return result
}
