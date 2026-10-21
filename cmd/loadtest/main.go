// loadtest generates a reproducible, closed-loop workload and verifies drain.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yourname/asynq-task-queue/internal/jobs"
)

type reply struct {
	Data struct {
		TaskID string   `json:"task_id"`
		Job    jobs.Job `json:"job"`
	} `json:"data"`
}
type report struct {
	Run              string    `json:"run"`
	StartedAt        time.Time `json:"started_at"`
	GoVersion        string    `json:"go_version"`
	Platform         string    `json:"platform"`
	Requests         int       `json:"requests"`
	UniqueJobs       int       `json:"unique_jobs"`
	Concurrency      int       `json:"client_concurrency"`
	Workers          int       `json:"worker_replicas"`
	Accepted         int       `json:"accepted"`
	Replayed         int       `json:"replayed"`
	Errors           int       `json:"errors"`
	Completed        int       `json:"completed"`
	Effects          int       `json:"effects"`
	EffectsVerified  bool      `json:"effects_verified"`
	AdmissionSeconds float64   `json:"admission_seconds"`
	DrainSeconds     float64   `json:"total_seconds_until_drain"`
	AdmissionRPS     float64   `json:"admission_requests_per_second"`
	CompletionRPS    float64   `json:"completed_jobs_per_second"`
	AdmissionP50     float64   `json:"admission_p50_ms"`
	AdmissionP95     float64   `json:"admission_p95_ms"`
	AdmissionP99     float64   `json:"admission_p99_ms"`
	CompletionP95    float64   `json:"completion_p95_ms"`
}

func main() {
	base := flag.String("url", "http://localhost:8080", "API base URL")
	requests := flag.Int("requests", 1000, "total POSTs")
	unique := flag.Int("unique", 1000, "distinct idempotency keys")
	concurrency := flag.Int("concurrency", 20, "concurrent HTTP clients")
	workers := flag.Int("workers", 1, "actual worker replica count recorded in report")
	timeout := flag.Duration("timeout", 2*time.Minute, "total workload deadline")
	dsn := flag.String("database-url", os.Getenv("DATABASE_URL"), "optional PostgreSQL DSN for independent effect verification")
	flag.Parse()
	if *requests < 1 || *unique < 1 || *unique > *requests || *concurrency < 1 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid workload parameters")
		os.Exit(2)
	}
	r := report{Run: uuid.NewString(), StartedAt: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Requests: *requests, UniqueJobs: *unique, Concurrency: *concurrency, Workers: *workers}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency}}
	payload := []byte(`{"user_id":42,"to_address":"load@example.com","template_id":"load","subject":"Load test","queue":"default"}`)
	started := time.Now()
	indices := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]float64, 0, *requests)
	ids := make(map[int]string)
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range indices {
				key := i % *unique
				req, _ := http.NewRequestWithContext(ctx, "POST", *base+"/api/v1/tasks/email", bytes.NewReader(payload))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", fmt.Sprintf("load-%s-%d", r.Run, key))
				begin := time.Now()
				res, err := client.Do(req)
				elapsed := float64(time.Since(begin).Microseconds()) / 1000
				var body reply
				code := 0
				if err == nil {
					code = res.StatusCode
					err = json.NewDecoder(res.Body).Decode(&body)
					res.Body.Close()
				}
				mu.Lock()
				durations = append(durations, elapsed)
				if err != nil || (code != 202 && code != 200) || body.Data.TaskID == "" {
					r.Errors++
				} else {
					if prior, ok := ids[key]; ok && prior != body.Data.TaskID {
						r.Errors++
					}
					ids[key] = body.Data.TaskID
					if code == 202 {
						r.Accepted++
					} else {
						r.Replayed++
					}
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < *requests; i++ {
		indices <- i
	}
	close(indices)
	wg.Wait()
	r.AdmissionSeconds = time.Since(started).Seconds()
	r.AdmissionRPS = float64(*requests) / r.AdmissionSeconds
	r.AdmissionP50 = percentile(durations, .5)
	r.AdmissionP95 = percentile(durations, .95)
	r.AdmissionP99 = percentile(durations, .99)
	completionTimes := make([]float64, 0, len(ids))
	if *dsn != "" {
		completionTimes = drainDatabase(ctx, *dsn, ids, &r)
	} else {
		sem := make(chan struct{}, *concurrency)
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				for ctx.Err() == nil {
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						return
					}
					req, _ := http.NewRequestWithContext(ctx, "GET", *base+"/api/v1/jobs/"+id, nil)
					res, err := client.Do(req)
					var body reply
					if err == nil {
						err = json.NewDecoder(res.Body).Decode(&body)
						res.Body.Close()
					}
					<-sem
					if err == nil && body.Data.Job.Status == "completed" && body.Data.Job.CompletedAt != nil {
						mu.Lock()
						r.Completed++
						completionTimes = append(completionTimes, float64(body.Data.Job.CompletedAt.Sub(body.Data.Job.CreatedAt).Microseconds())/1000)
						mu.Unlock()
						return
					}
					if err == nil && (body.Data.Job.Status == "archived" || body.Data.Job.Status == "canceled") {
						return
					}
					timer := time.NewTimer(200 * time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}(id)
		}
		wg.Wait()
	}
	r.DrainSeconds = time.Since(started).Seconds()
	r.CompletionRPS = float64(r.Completed) / r.DrainSeconds
	r.CompletionP95 = percentile(completionTimes, .95)
	if *dsn != "" {
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer verifyCancel()
		s, err := jobs.Open(verifyCtx, *dsn, 4)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			r.Errors++
		} else {
			jobIDs := make([]string, 0, len(ids))
			for _, id := range ids {
				jobIDs = append(jobIDs, id)
			}
			err = s.Pool.QueryRow(verifyCtx, "SELECT count(*) FROM job_effects WHERE job_id::text=ANY($1)", jobIDs).Scan(&r.Effects)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				r.Errors++
			} else {
				r.EffectsVerified = r.Effects == *unique
			}
			s.Close()
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(r)
	if r.Errors != 0 || r.Completed != *unique || r.Accepted != *unique || (*dsn != "" && !r.EffectsVerified) {
		os.Exit(1)
	}
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	i := int(float64(len(values)-1) * p)
	return values[i]
}

// Database polling avoids adding thousands of history HTTP requests to the
// measured workload. The admission API remains the only source of submitted IDs.
func drainDatabase(ctx context.Context, dsn string, ids map[int]string, r *report) []float64 {
	s, err := jobs.Open(ctx, dsn, 4)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		r.Errors++
		return nil
	}
	defer s.Close()
	jobIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		jobIDs = append(jobIDs, id)
	}
	var durations []float64
	for ctx.Err() == nil {
		rows, err := s.Pool.Query(ctx, "SELECT created_at,completed_at FROM jobs WHERE id::text=ANY($1) AND status='completed'", jobIDs)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			r.Errors++
			return nil
		}
		durations = nil
		for rows.Next() {
			var created, completed time.Time
			if err = rows.Scan(&created, &completed); err != nil {
				break
			}
			durations = append(durations, float64(completed.Sub(created).Microseconds())/1000)
		}
		rowErr := rows.Err()
		rows.Close()
		if err != nil || rowErr != nil {
			r.Errors++
			return nil
		}
		r.Completed = len(durations)
		if r.Completed == len(ids) {
			return durations
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return durations
		case <-timer.C:
		}
	}
	return durations
}
