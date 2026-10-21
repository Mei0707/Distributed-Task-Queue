package jobs

import (
	"fmt"
	"github.com/google/uuid"
	"strconv"
	"strings"
)

// Generation one keeps the original UUID transport ID for existing jobs.
func TransportID(id string, generation int64) string {
	if generation == 1 {
		return id
	}
	return fmt.Sprintf("%s:%d", id, generation)
}
func ParseTaskID(taskID string) (string, int64, error) {
	parts := strings.Split(taskID, ":")
	if len(parts) > 2 {
		return "", 0, ErrNotFound
	}
	parsed, err := uuid.Parse(parts[0])
	if err != nil {
		return "", 0, err
	}
	generation := int64(1)
	if len(parts) == 2 {
		generation, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || generation < 2 {
			return "", 0, ErrNotFound
		}
	}
	return parsed.String(), generation, nil
}
