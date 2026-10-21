package email

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mei0707/Distributed-Task-Queue/internal/jobs"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

// TypeEmailDelivery is the unique task type identifier.
// The ServeMux routes tasks to handlers by this string.
const TypeEmailDelivery = "email:deliver"

// DeliveryPayload is the data serialized into the task.
// Keep payloads small — store IDs, not full objects.
type DeliveryPayload struct {
	UserID     int    `json:"user_id"`
	ToAddress  string `json:"to_address"`
	TemplateID string `json:"template_id"`
	Subject    string `json:"subject"`
}

// NewDeliveryTask creates an asynq.Task ready to be enqueued.
// Options set here are defaults; they can be overridden at enqueue time.
func NewDeliveryTask(payload DeliveryPayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("email: failed to marshal payload: %w", err)
	}

	return asynq.NewTask(
		TypeEmailDelivery,
		data,
		asynq.MaxRetry(5),             // retry up to 5 times on failure
		asynq.Timeout(30*time.Second), // cancel if handler takes > 30s
		asynq.Retention(72*time.Hour), // keep completed task in Redis for 72h
	), nil
}

// Handler processes email delivery tasks.
// It implements the asynq.Handler interface via ProcessTask.
type Handler struct {
	logger *zap.Logger
}

func NewHandler(logger *zap.Logger) *Handler {
	return &Handler{logger: logger}
}

// ProcessTask is called by the worker for each email:deliver task.
// Returning a non-nil error causes the task to be retried (up to MaxRetry).
// Wrapping with asynq.SkipRetry sends the task directly to the dead letter queue.
func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	// 1. Deserialize the payload
	var p DeliveryPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		// Malformed payload — skip retry, it will never succeed
		return fmt.Errorf("email: invalid payload: %w: %w", err, asynq.SkipRetry)
	}

	h.logger.Info("Processing email delivery task",
		zap.String("task_id", taskID(ctx)),
		zap.Int("user_id", p.UserID),
		zap.String("to", p.ToAddress),
		zap.String("template", p.TemplateID),
	)

	// 2. Check context cancellation (respects task Timeout/Deadline)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 3. Perform actual email delivery
	// In production: call your SMTP provider (SendGrid, SES, Postmark, etc.)
	if err := sendEmail(ctx, p); err != nil {
		// Transient error — allow retry
		return fmt.Errorf("email: delivery failed for user %d: %w", p.UserID, err)
	}

	if err := jobs.RecordEffect(ctx, "simulated_email_delivery", map[string]any{"delivered_to": p.ToAddress, "delivered_at": time.Now().UTC(), "simulated": true}); err != nil {
		return err
	}

	h.logger.Info("Email delivered successfully",
		zap.String("to", p.ToAddress),
		zap.Int("user_id", p.UserID),
	)

	return nil
}

// sendEmail is a stub for your real email provider integration.
func sendEmail(ctx context.Context, p DeliveryPayload) error {
	// TODO: Replace with real SMTP/API call
	// Example with SendGrid:
	//   client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	//   message := mail.NewSingleEmail(from, p.Subject, to, plainText, htmlContent)
	//   _, err := client.Send(message)

	// Simulate work
	return wait(ctx, 100*time.Millisecond)
}
func taskID(ctx context.Context) string { id, _ := asynq.GetTaskID(ctx); return id }

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
