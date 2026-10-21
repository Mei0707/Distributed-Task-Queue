package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
	"github.com/yourname/asynq-task-queue/internal/telemetry"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

type SubmissionHandler struct {
	store  *jobs.Store
	worker config.WorkerConfig
	logger *zap.Logger
}

func (h *SubmissionHandler) Submit(c *gin.Context, t *asynq.Task, queue string, delay int) {
	if queue == "" {
		queue = "default"
	}
	key := c.GetHeader("Idempotency-Key")
	if len(key) > 200 {
		response.BadRequest(c, "Idempotency-Key must be at most 200 bytes")
		return
	}
	if key == "" {
		key = uuid.NewString()
	}
	c.Header("Idempotency-Key", key)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	j, replayed, err := h.store.Accept(ctx, jobs.Submission{Type: t.Type(), Key: key, Payload: t.Payload(), Queue: queue, DelaySeconds: delay, MaxRetry: h.worker.MaxRetry, TimeoutSeconds: int(h.worker.TaskTimeout / time.Second), RetentionSeconds: int(h.worker.RetentionTTL / time.Second)})
	if errors.Is(err, jobs.ErrConflict) {
		telemetry.Submissions.WithLabelValues(t.Type(), "conflict").Inc()
		c.JSON(http.StatusConflict, response.APIResponse{Error: err.Error()})
		return
	}
	if err != nil {
		h.logger.Error("Job acceptance failed", zap.Error(err))
		telemetry.Submissions.WithLabelValues(t.Type(), "error").Inc()
		c.JSON(http.StatusServiceUnavailable, response.APIResponse{Error: "Job storage unavailable; retry with the same Idempotency-Key"})
		return
	}
	code := http.StatusAccepted
	outcome := "accepted"
	if replayed {
		code = http.StatusOK
		outcome = "replayed"
	}
	telemetry.Submissions.WithLabelValues(t.Type(), outcome).Inc()
	c.Header("Location", "/api/v1/jobs/"+j.ID)
	c.Header("Idempotency-Replayed", map[bool]string{true: "true", false: "false"}[replayed])
	c.JSON(code, response.APIResponse{Success: true, Data: response.TaskEnqueuedData{TaskID: j.ID, Queue: j.Queue, Status: j.Status}})
}

func (h *SubmissionHandler) List(c *gin.Context) {
	page, size := getPagination(c)
	if page > 100000 {
		response.BadRequest(c, "page too large")
		return
	}
	result, err := h.store.List(c.Request.Context(), size, (page-1)*size)
	if err != nil {
		response.InternalError(c, "Failed to read job history")
		return
	}
	response.OK(c, gin.H{"jobs": result, "page": page, "page_size": size})
}
func validJobID(c *gin.Context) bool {
	if _, err := uuid.Parse(c.Param("id")); err != nil {
		response.BadRequest(c, "Invalid job ID")
		return false
	}
	return true
}
func (h *SubmissionHandler) Get(c *gin.Context) {
	if !validJobID(c) {
		return
	}
	j, err := h.store.Get(c.Request.Context(), c.Param("id"))
	if errors.Is(err, jobs.ErrNotFound) {
		c.JSON(404, response.APIResponse{Error: "Job not found"})
		return
	}
	if err != nil {
		response.InternalError(c, "Failed to read job history")
		return
	}
	attempts, err := h.store.Attempts(c.Request.Context(), j.ID)
	if err != nil {
		response.InternalError(c, "Failed to read attempts")
		return
	}
	var result json.RawMessage
	// A missing effect is expected for a job that has not completed.
	err = h.store.Pool.QueryRow(c.Request.Context(), "SELECT COALESCE((SELECT result FROM job_effects WHERE job_id=$1),'null'::jsonb)", j.ID).Scan(&result)
	if err != nil {
		response.InternalError(c, "Failed to read result")
		return
	}
	response.OK(c, gin.H{"job": j, "attempts": attempts, "result": result})
}
func (h *SubmissionHandler) Cancel(c *gin.Context) {
	if !validJobID(c) {
		return
	}
	err := h.store.Cancel(c.Request.Context(), c.Param("id"))
	if errors.Is(err, jobs.ErrConflict) {
		c.JSON(409, response.APIResponse{Error: "Job already completed, canceled, or missing"})
		return
	}
	if err != nil {
		response.InternalError(c, "Failed to cancel job")
		return
	}
	response.OK(c, gin.H{"status": "canceled"})
}
