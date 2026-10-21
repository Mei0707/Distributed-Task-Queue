package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIResponse is the standard envelope for all API responses.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// TaskEnqueuedData is returned when a task is successfully enqueued.
type TaskEnqueuedData struct {
	TaskID string `json:"task_id"`
	Queue  string `json:"queue"`
	Status string `json:"status"`
}

func OK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: data})
}

func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, APIResponse{Success: true, Data: data})
}

func BadRequest(c *gin.Context, err string) {
	c.JSON(http.StatusBadRequest, APIResponse{Success: false, Error: err})
}

func InternalError(c *gin.Context, err string) {
	c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err})
}
