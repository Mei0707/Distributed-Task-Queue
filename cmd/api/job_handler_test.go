package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
	"go.uber.org/zap"
)

func apiStore(t *testing.T) *jobs.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := jobs.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_api_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, err := jobs.Open(ctx, u.String(), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		admin.Pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestIdempotentHTTPSubmissionAndHistory(t *testing.T) {
	s := apiStore(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(nil, nil, zap.NewNop(), s, cfg)
	body := `{"user_id":42,"to_address":"test@example.com","template_id":"welcome","subject":"hello"}`
	post := func(payload, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/email", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	first := post(body, "same-key")
	if first.Code != 202 {
		t.Fatalf("new submission: %d %s", first.Code, first.Body.String())
	}
	replay := post(body, "same-key")
	if replay.Code != 200 || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body.String())
	}
	var a, b struct {
		Data struct {
			TaskID string `json:"task_id"`
		} `json:"data"`
	}
	json.Unmarshal(first.Body.Bytes(), &a)
	json.Unmarshal(replay.Body.Bytes(), &b)
	if a.Data.TaskID == "" || a.Data.TaskID != b.Data.TaskID {
		t.Fatal("replay changed job ID")
	}
	if rec := post(strings.Replace(body, "hello", "changed", 1), "same-key"); rec.Code != 409 {
		t.Fatalf("conflict status: %d", rec.Code)
	}
	if rec := post(body, strings.Repeat("k", 201)); rec.Code != 400 {
		t.Fatalf("oversized key: %d", rec.Code)
	}
	for _, extra := range []string{`,"queue":"unknown"`, `,"process_in_seconds":-1`, `,"process_in_seconds":604801`} {
		payload := strings.TrimSuffix(body, "}") + extra + "}"
		if rec := post(payload, uuid.NewString()); rec.Code != 400 {
			t.Fatalf("invalid options: %d %s", rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/jobs/"+a.Data.TaskID, nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
		t.Fatalf("history: %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/jobs/"+a.Data.TaskID+"/cancel", nil))
	if rec.Code != 200 {
		t.Fatalf("cancel: %s", rec.Body.String())
	}
	s.Close()
	if rec := post(body, uuid.NewString()); rec.Code != 503 {
		t.Fatalf("database failure should be retryable: %d", rec.Code)
	}
}

func TestTaskViewContract(t *testing.T) {
	// The API must serialize a lower-case task ID and actual JSON payload, rather
	// than exposing Asynq's exported field names and base64-encoded byte slices.
	view := taskViews([]*asynq.TaskInfo{{ID: "example", Type: "email:deliver", Payload: []byte(`{"value":42}`), State: asynq.TaskStatePending, Retried: 2}})
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"id":"example"`) || !strings.Contains(string(data), `"payload":{"value":42}`) || !strings.Contains(string(data), `"retry_count":2`) {
		t.Fatalf("bad dashboard contract: %s", data)
	}
}
