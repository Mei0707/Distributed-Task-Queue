package telemetry

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Registry = prometheus.NewRegistry()
var Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "queue_http_requests_total", Help: "HTTP requests by bounded route, method, and status."}, []string{"route", "method", "status"})
var RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "queue_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"route", "method"})
var Submissions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "queue_submissions_total", Help: "Durable submissions by task type and result."}, []string{"type", "result"})
var Executions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "queue_executions_total", Help: "Worker executions by task type and outcome."}, []string{"type", "outcome"})
var ExecutionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "queue_execution_duration_seconds", Help: "Execution latency including database lock wait.", Buckets: prometheus.DefBuckets}, []string{"type"})
var InFlight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "queue_worker_in_flight", Help: "Active worker executions."})
var OutboxFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: "queue_outbox_failures_total", Help: "Failed Redis enqueue attempts."})

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), Requests, RequestDuration, Submissions, Executions, ExecutionDuration, InFlight, OutboxFailures)
}
func Handler() http.Handler { return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}) }

// ObserveHTTP uses Gin's route pattern; raw job IDs never become metric labels.
func ObserveHTTP() gin.HandlerFunc {
	return func(c *gin.Context) {
		timer := prometheus.NewTimer(prometheus.ObserverFunc(func(seconds float64) {
			route := c.FullPath()
			if route == "" {
				route = "unmatched"
			}
			RequestDuration.WithLabelValues(route, c.Request.Method).Observe(seconds)
		}))
		c.Next()
		timer.ObserveDuration()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		Requests.WithLabelValues(route, c.Request.Method, http.StatusText(c.Writer.Status())).Inc()
	}
}
