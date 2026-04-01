package main

import (
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	imagetask "github.com/yourname/asynq-task-queue/internal/tasks/image"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

// ImageHandler handles HTTP requests for image processing task enqueueing.
type ImageHandler struct {
	client *asynq.Client
	logger *zap.Logger
}

func NewImageHandler(client *asynq.Client, logger *zap.Logger) *ImageHandler {
	return &ImageHandler{client: client, logger: logger}
}

// EnqueueImageRequest is the JSON body for POST /tasks/image/resize
type EnqueueImageRequest struct {
	SourceURL    string `json:"source_url" binding:"required,url"`
	OutputBucket string `json:"output_bucket" binding:"required"`
	OutputKey    string `json:"output_key" binding:"required"`
	Width        int    `json:"width" binding:"required,min=1,max=8000"`
	Height       int    `json:"height" binding:"required,min=1,max=8000"`
	Format       string `json:"format" binding:"required,oneof=jpeg png webp"`

	// Scheduling options (same pattern as email)
	ProcessIn int    `json:"process_in_seconds"`
	Queue     string `json:"queue"` // Recommend "low" for batch jobs, "critical" for user-facing
}

// EnqueueResize godoc
// @Summary      Enqueue an image resize task
// @Description  Downloads, resizes, and re-uploads an image asynchronously
// @Tags         image
// @Accept       json
// @Produce      json
// @Param        body  body  EnqueueImageRequest  true  "Image resize payload"
// @Success      201   {object}  response.APIResponse
// @Failure      400   {object}  response.APIResponse
// @Failure      500   {object}  response.APIResponse
// @Router       /tasks/image/resize [post]
func (h *ImageHandler) EnqueueResize(c *gin.Context) {
	var req EnqueueImageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request body: "+err.Error())
		return
	}

	payload := imagetask.ResizePayload{
		SourceURL:    req.SourceURL,
		OutputBucket: req.OutputBucket,
		OutputKey:    req.OutputKey,
		Width:        req.Width,
		Height:       req.Height,
		Format:       req.Format,
	}

	task, err := imagetask.NewResizeTask(payload)
	if err != nil {
		h.logger.Error("Failed to create image resize task", zap.Error(err))
		response.InternalError(c, "Failed to create task")
		return
	}

	opts := buildEnqueueOptions(req.Queue, req.ProcessIn)

	info, err := h.client.Enqueue(task, opts...)
	if err != nil {
		h.logger.Error("Failed to enqueue image resize task",
			zap.Error(err),
			zap.String("source", req.SourceURL),
		)
		response.InternalError(c, "Failed to enqueue task: "+err.Error())
		return
	}

	h.logger.Info("Image resize task enqueued",
		zap.String("task_id", info.ID),
		zap.String("queue", info.Queue),
		zap.String("source", req.SourceURL),
	)

	response.Created(c, response.TaskEnqueuedData{
		TaskID: info.ID,
		Queue:  info.Queue,
		Status: info.State.String(),
	})
}
