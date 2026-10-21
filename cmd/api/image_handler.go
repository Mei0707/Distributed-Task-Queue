package main

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	imagetask "github.com/yourname/asynq-task-queue/internal/tasks/image"
	"github.com/yourname/asynq-task-queue/pkg/response"
)

// ImageHandler handles HTTP requests for image processing task enqueueing.
type ImageHandler struct {
	submitter *SubmissionHandler
	logger    *zap.Logger
}

func NewImageHandler(submitter *SubmissionHandler, logger *zap.Logger) *ImageHandler {
	return &ImageHandler{submitter: submitter, logger: logger}
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
	ProcessIn int    `json:"process_in_seconds" binding:"min=0,max=604800"`
	Queue     string `json:"queue" binding:"omitempty,oneof=critical default low"` // Recommend "low" for batch jobs, "critical" for user-facing
}

// EnqueueResize godoc
// @Summary      Enqueue an image resize task
// @Description  Downloads, resizes, and re-uploads an image asynchronously
// @Tags         image
// @Accept       json
// @Produce      json
// @Param        body  body  EnqueueImageRequest  true  "Image resize payload"
// @Success      202   {object}  response.APIResponse
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

	h.submitter.Submit(c, task, req.Queue, req.ProcessIn)
}
