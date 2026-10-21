package image

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mei0707/Distributed-Task-Queue/internal/jobs"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

const TypeImageResize = "image:resize"

// ResizePayload describes the image processing job.
// SourceURL points to the original; OutputBucket is the S3/GCS destination.
type ResizePayload struct {
	SourceURL    string `json:"source_url"`
	OutputBucket string `json:"output_bucket"`
	OutputKey    string `json:"output_key"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Format       string `json:"format"` // "jpeg", "png", "webp"
}

// NewResizeTask creates an image resize task.
// Image processing is CPU/IO-intensive, so we allow a longer timeout.
func NewResizeTask(payload ResizePayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("image: failed to marshal payload: %w", err)
	}

	return asynq.NewTask(
		TypeImageResize,
		data,
		asynq.MaxRetry(3),            // fewer retries — storage ops can be expensive
		asynq.Timeout(5*time.Minute), // image processing can be slow
		asynq.Retention(24*time.Hour),
	), nil
}

// Handler processes image resize tasks.
type Handler struct {
	logger *zap.Logger
}

func NewHandler(logger *zap.Logger) *Handler {
	return &Handler{logger: logger}
}

func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var p ResizePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("image: invalid payload: %w: %w", err, asynq.SkipRetry)
	}

	h.logger.Info("Processing image resize task",
		zap.String("task_id", taskID(ctx)),
		zap.String("source", p.SourceURL),
		zap.Int("width", p.Width),
		zap.Int("height", p.Height),
		zap.String("format", p.Format),
	)

	// Step 1: Download source image
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := downloadImage(ctx, p.SourceURL); err != nil {
		return fmt.Errorf("image: download failed: %w", err)
	}

	// Step 2: Resize
	outputPath, err := resizeImage(ctx, p)
	if err != nil {
		return fmt.Errorf("image: resize failed: %w", err)
	}

	// Step 3: Upload to storage
	if err := uploadImage(ctx, outputPath, p.OutputBucket, p.OutputKey); err != nil {
		return fmt.Errorf("image: upload failed: %w", err)
	}

	if err := jobs.RecordEffect(ctx, "simulated_image_artifact", map[string]any{"output_bucket": p.OutputBucket, "output_key": p.OutputKey, "processed_at": time.Now().UTC(), "simulated": true}); err != nil {
		return err
	}

	h.logger.Info("Image resized and uploaded successfully",
		zap.String("output", fmt.Sprintf("s3://%s/%s", p.OutputBucket, p.OutputKey)),
	)

	return nil
}

// --- Stubs for real implementation ---

func downloadImage(ctx context.Context, url string) error {
	// TODO: Use http.Get or AWS S3 SDK to download
	_ = url
	return wait(ctx, 200*time.Millisecond)
}

func resizeImage(ctx context.Context, p ResizePayload) (string, error) {
	// TODO: Use github.com/disintegration/imaging or similar
	// img, err := imaging.Open(localPath)
	// resized := imaging.Resize(img, p.Width, p.Height, imaging.Lanczos)
	// imaging.Save(resized, outputPath)
	_ = p
	if err := wait(ctx, 500*time.Millisecond); err != nil {
		return "", err
	}
	return "/tmp/output.jpg", nil
}

func uploadImage(ctx context.Context, localPath, bucket, key string) error {
	// TODO: Use AWS SDK v2
	// uploader := manager.NewUploader(s3Client)
	// uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: file})
	_, _, _ = localPath, bucket, key
	return wait(ctx, 300*time.Millisecond)
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
