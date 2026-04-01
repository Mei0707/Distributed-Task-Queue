package email

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
)

func TestNewDeliveryTask(t *testing.T) {
	t.Parallel()

	payload := DeliveryPayload{
		UserID:     7,
		ToAddress:  "user@example.com",
		TemplateID: "welcome_email",
		Subject:    "Welcome!",
	}

	task, err := NewDeliveryTask(payload)
	if err != nil {
		t.Fatalf("expected task creation to succeed: %v", err)
	}
	if task.Type() != TypeEmailDelivery {
		t.Fatalf("expected type %q, got %q", TypeEmailDelivery, task.Type())
	}

	var got DeliveryPayload
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
	task := asynq.NewTask(TypeEmailDelivery, []byte("not-json"))

	err := handler.ProcessTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected skip retry error, got %v", err)
	}
}
