package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
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

var ProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "eds_processing_duration_seconds",
	Help:    "The latency in duration of processing events from receving them to flushing them",
	Buckets: []float64{1, 2, 3, 5, 10, 60, 300, 600, 1800, 3600},
})

// SystemStats contains the metrics and system stats
type SystemStats struct {
	Metrics struct {
		FlushCount         float64 `json:"flushCount"`
		FlushDuration      float64 `json:"flushDuration"`
		ProcessingDuration float64 `json:"processingDuration"`
		PendingEvents      float64 `json:"pendingEvents"`
		TotalEvents        float64 `json:"totalEvents"`
	} `json:"metrics"`
	Memory *mem.VirtualMemoryStat `json:"memory"`
	Load   *load.AvgStat          `json:"load"`
}

// collect calls the function for each metric associated with the Collector
func collect(col prometheus.Collector, do func(*dto.Metric)) {
	c := make(chan prometheus.Metric)
	go func(c chan prometheus.Metric) {
		col.Collect(c)
		close(c)
	}(c)
	for x := range c { // eg range across distinct label vector values
		m := dto.Metric{}
		_ = x.Write(&m)
		do(&m)
	}
}

// getMetricValue returns the sum of the Counter metrics associated with the Collector
// e.g. the metric for a non-vector, or the sum of the metrics for vector labels.
// If the metric is a Histogram then number of samples is used.
func getMetricValue(col prometheus.Collector) float64 {
	var total float64
	collect(col, func(m *dto.Metric) {
		if h := m.GetHistogram(); h != nil {
			total += float64(h.GetSampleCount())
		} else {
			total += m.GetCounter().GetValue()
		}
	})
	return total
}

// GetSystemStats returns a snapshot of the system stats
func GetSystemStats() (*SystemStats, error) {
	var s SystemStats
	var err error
	s.Metrics.FlushCount = getMetricValue(FlushCount)
	s.Metrics.FlushDuration = getMetricValue(FlushDuration)
	s.Metrics.PendingEvents = getMetricValue(PendingEvents)
	s.Metrics.TotalEvents = getMetricValue(TotalEvents)
	s.Metrics.ProcessingDuration = getMetricValue(ProcessingDuration)
	s.Memory, err = mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	s.Load, err = load.Avg()
	return &s, err
}
