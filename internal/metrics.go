package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var PendingEvents = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "eds_pending_events",
	Help: "The number of pending events",
})

var TotalEvents = promauto.NewCounter(prometheus.CounterOpts{
	Name: "eds_total_events",
	Help: "The total number of events processed",
})

var FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "eds_flush_duration_seconds",
	Help:    "The duration of driver flushes",
	Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
})

var FlushCount = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "eds_flush_count",
	Help:    "The count of events flushed",
	Buckets: []float64{1, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
})
