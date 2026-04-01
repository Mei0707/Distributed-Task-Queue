package main

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	emailtask "github.com/yourname/asynq-task-queue/internal/tasks/email"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

// EmailHandler handles HTTP requests for email task enqueueing.
type EmailHandler struct {
	client *asynq.Client
	logger *zap.Logger
}

func NewEmailHandler(client *asynq.Client, logger *zap.Logger) *EmailHandler {
	return &EmailHandler{client: client, logger: logger}
}

// EnqueueEmailRequest is the JSON body for POST /tasks/email
type EnqueueEmailRequest struct {
	UserID     int    `json:"user_id" binding:"required"`
	ToAddress  string `json:"to_address" binding:"required,email"`
	TemplateID string `json:"template_id" binding:"required"`
	Subject    string `json:"subject" binding:"required"`

	// Optional scheduling options
	// ProcessIn: delay in seconds from now (e.g. 3600 = in 1 hour)
	// Queue: override default queue ("critical", "default", "low")
	ProcessIn int    `json:"process_in_seconds"` // 0 = immediate
	Queue     string `json:"queue"`              // "" = "default"
}

// Enqueue godoc
// @Summary      Enqueue an email delivery task
// @Description  Creates an email delivery task and pushes it onto the task queue
// @Tags         email
// @Accept       json
// @Produce      json
// @Param        body  body  EnqueueEmailRequest  true  "Email task payload"
// @Success      201   {object}  response.APIResponse
// @Failure      400   {object}  response.APIResponse
// @Failure      500   {object}  response.APIResponse
// @Router       /tasks/email [post]
func (h *EmailHandler) Enqueue(c *gin.Context) {
	var req EnqueueEmailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request body: "+err.Error())
		return
	}

	// Build asynq task
	payload := emailtask.DeliveryPayload{
		UserID:     req.UserID,
		ToAddress:  req.ToAddress,
		TemplateID: req.TemplateID,
		Subject:    req.Subject,
	}

	task, err := emailtask.NewDeliveryTask(payload)
	if err != nil {
		h.logger.Error("Failed to create email task", zap.Error(err))
		response.InternalError(c, "Failed to create task")
		return
	}

	// Build enqueue options
	opts := buildEnqueueOptions(req.Queue, req.ProcessIn)

	// Enqueue returns TaskInfo with the assigned ID and queue
	info, err := h.client.Enqueue(task, opts...)
	if err != nil {
		h.logger.Error("Failed to enqueue email task",
			zap.Error(err),
			zap.Int("user_id", req.UserID),
		)
		response.InternalError(c, "Failed to enqueue task: "+err.Error())
		return
	}

	h.logger.Info("Email task enqueued",
		zap.String("task_id", info.ID),
		zap.String("queue", info.Queue),
		zap.Int("user_id", req.UserID),
	)

	response.Created(c, response.TaskEnqueuedData{
		TaskID: info.ID,
		Queue:  info.Queue,
		Status: info.State.String(),
	})
}

// buildEnqueueOptions translates HTTP request fields into asynq options.
func buildEnqueueOptions(queue string, processInSeconds int) []asynq.Option {
	var opts []asynq.Option

	// Queue routing
	if queue != "" {
		opts = append(opts, asynq.Queue(queue))
	} else {
		opts = append(opts, asynq.Queue("default"))
	}

	// Delayed scheduling
	if processInSeconds > 0 {
		opts = append(opts, asynq.ProcessIn(time.Duration(processInSeconds)*time.Second))
	}

	return opts
}
