package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yourname/asynq-task-queue/internal/config"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) { gin.SetMode(gin.TestMode); os.Exit(m.Run()) }

func TestEmailEnqueueValidationError(t *testing.T) {
	t.Parallel()

	handler := NewEmailHandler(nil, zap.NewNop())

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/email",
		strings.NewReader(`{"user_id":42,"to_address":"not-an-email","template_id":"welcome_email","subject":"Welcome!"}`),
	)
	ctx.Request.Header.Set("Content-Type", "application/json")

	handler.Enqueue(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d, body=%s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON response: %v", err)
	}
	if resp["success"] != false {
		t.Fatalf("expected success=false, got %v", resp["success"])
	}
}

func TestDashboardRoute(t *testing.T) {
	t.Parallel()

	router := NewRouter(nil, nil, zap.NewNop(), nil, &config.Config{})

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Queue Dashboard") {
		t.Fatalf("expected dashboard HTML response, got %q", rec.Body.String())
	}
}
