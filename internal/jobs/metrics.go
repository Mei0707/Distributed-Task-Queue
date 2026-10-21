package jobs

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector reads durable state at scrape time, including while workers are down.
type Collector struct{ Store *Store }

var jobsDesc = prometheus.NewDesc("queue_jobs", "Durable jobs by status.", []string{"status"}, nil)
var outboxDesc = prometheus.NewDesc("queue_outbox_pending", "Undelivered outbox entries.", nil, nil)
var oldestDesc = prometheus.NewDesc("queue_outbox_oldest_seconds", "Age of oldest undelivered outbox entry.", nil, nil)
var databaseDesc = prometheus.NewDesc("queue_database_up", "Whether durable metrics could be read.", nil, nil)

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jobsDesc
	ch <- outboxDesc
	ch <- oldestDesc
	ch <- databaseDesc
}
func (c Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts := map[string]float64{"accepted": 0, "retry": 0, "completed": 0, "archived": 0, "canceled": 0}
	rows, err := c.Store.Pool.Query(ctx, "SELECT status,count(*) FROM jobs GROUP BY status")
	if err != nil {
		ch <- prometheus.MustNewConstMetric(databaseDesc, prometheus.GaugeValue, 0)
		return
	}
	for rows.Next() {
		var state string
		var n float64
		if err = rows.Scan(&state, &n); err != nil {
			break
		}
		counts[state] = n
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil || rowErr != nil {
		ch <- prometheus.MustNewConstMetric(databaseDesc, prometheus.GaugeValue, 0)
		return
	}
	var pending, oldest float64
	err = c.Store.Pool.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM now()-min(j.created_at)),0) FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE delivered_at IS NULL`).Scan(&pending, &oldest)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(databaseDesc, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(databaseDesc, prometheus.GaugeValue, 1)
	for state, n := range counts {
		ch <- prometheus.MustNewConstMetric(jobsDesc, prometheus.GaugeValue, n, state)
	}
	ch <- prometheus.MustNewConstMetric(outboxDesc, prometheus.GaugeValue, pending)
	ch <- prometheus.MustNewConstMetric(oldestDesc, prometheus.GaugeValue, oldest)
}
