package main

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	emailtask "github.com/yourname/asynq-task-queue/internal/tasks/email"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

// EmailHandler handles HTTP requests for email task enqueueing.
type EmailHandler struct {
	submitter *SubmissionHandler
	logger    *zap.Logger
}

func NewEmailHandler(submitter *SubmissionHandler, logger *zap.Logger) *EmailHandler {
	return &EmailHandler{submitter: submitter, logger: logger}
}

// EnqueueEmailRequest is the JSON body for POST /tasks/email
type EnqueueEmailRequest struct {
	UserID     int    `json:"user_id" binding:"required,min=1"`
	ToAddress  string `json:"to_address" binding:"required,email"`
	TemplateID string `json:"template_id" binding:"required"`
	Subject    string `json:"subject" binding:"required"`

	// Optional scheduling options
	// ProcessIn: delay in seconds from now (e.g. 3600 = in 1 hour)
	// Queue: override default queue ("critical", "default", "low")
	ProcessIn int    `json:"process_in_seconds" binding:"min=0,max=604800"`        // 0 = immediate
	Queue     string `json:"queue" binding:"omitempty,oneof=critical default low"` // "" = "default"
}

// Enqueue godoc
// @Summary      Enqueue an email delivery task
// @Description  Creates an email delivery task and pushes it onto the task queue
// @Tags         email
// @Accept       json
// @Produce      json
// @Param        body  body  EnqueueEmailRequest  true  "Email task payload"
// @Success      202   {object}  response.APIResponse
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

	h.submitter.Submit(c, task, req.Queue, req.ProcessIn)
}
