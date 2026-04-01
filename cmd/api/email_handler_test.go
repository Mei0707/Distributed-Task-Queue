package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func TestEmailEnqueueValidationError(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
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

func TestBuildEnqueueOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts := buildEnqueueOptions("", 0)
	if len(opts) != 1 {
		t.Fatalf("expected 1 option, got %d", len(opts))
	}
}

func TestBuildEnqueueOptionsQueueAndDelay(t *testing.T) {
	t.Parallel()

	opts := buildEnqueueOptions("low", 30)
	if len(opts) != 2 {
		t.Fatalf("expected 2 options, got %d", len(opts))
	}
}

func TestDashboardRoute(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	router := NewRouter(nil, nil, zap.NewNop())

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
