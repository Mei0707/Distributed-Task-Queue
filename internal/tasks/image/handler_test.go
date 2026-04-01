package image

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
)

func TestNewResizeTask(t *testing.T) {
	t.Parallel()

	payload := ResizePayload{
		SourceURL:    "https://example.com/image.jpg",
		OutputBucket: "bucket",
		OutputKey:    "resized/image.webp",
		Width:        800,
		Height:       600,
		Format:       "webp",
	}

	task, err := NewResizeTask(payload)
	if err != nil {
		t.Fatalf("expected task creation to succeed: %v", err)
	}
	if task.Type() != TypeImageResize {
		t.Fatalf("expected type %q, got %q", TypeImageResize, task.Type())
	}

	var got ResizePayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("expected payload to unmarshal: %v", err)
	}
	if got != payload {
		t.Fatalf("expected payload %+v, got %+v", payload, got)
	}
}

func TestProcessTaskInvalidPayloadSkipsRetry(t *testing.T) {
	t.Parallel()

	handler := NewHandler(nil)
	task := asynq.NewTask(TypeImageResize, []byte("not-json"))

	err := handler.ProcessTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected skip retry error, got %v", err)
	}
}
