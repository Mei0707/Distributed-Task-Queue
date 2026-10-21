package main

import (
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

type transportCollector struct{ inspector *asynq.Inspector }

var redisUp = prometheus.NewDesc("queue_redis_up", "Whether Redis inspection succeeded.", nil, nil)
var depth = prometheus.NewDesc("queue_transport_tasks", "Redis tasks by queue and state.", []string{"queue", "state"}, nil)

func (c transportCollector) Describe(ch chan<- *prometheus.Desc) { ch <- redisUp; ch <- depth }
func (c transportCollector) Collect(ch chan<- prometheus.Metric) {
	queues, err := c.inspector.Queues()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(redisUp, prometheus.GaugeValue, 0)
		return
	}
	for _, queue := range queues {
		info, err := c.inspector.GetQueueInfo(queue)
		if err != nil {
			ch <- prometheus.MustNewConstMetric(redisUp, prometheus.GaugeValue, 0)
			return
		}
		for state, n := range map[string]int{"pending": info.Pending, "active": info.Active, "scheduled": info.Scheduled, "retry": info.Retry, "archived": info.Archived, "completed": info.Completed} {
			ch <- prometheus.MustNewConstMetric(depth, prometheus.GaugeValue, float64(n), queue, state)
		}
	}
	ch <- prometheus.MustNewConstMetric(redisUp, prometheus.GaugeValue, 1)
}
