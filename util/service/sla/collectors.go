package rocserv

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shawnfeng/sutil/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	DefaultCollectorsOpts = CollectorsOpts{
		Port:        0,
		Location:    "/metrics",
		DefBuckets:  []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		DefQuantile: map[float64]float64{.25: .01, .5: .01, .75: .01, .9: .01, .95: .001, .99: .001},
	}
)

type CollectorType int32

// Possible values for the ValueType enum.
const (
	_ CollectorType = iota
	Counter
	Gauge
	Histogram
	Summary
	Untyped
)

// CollectorsOpts is used to configure the Collectors
type CollectorsOpts struct {
	Port        int
	Location    string
	DefBuckets  []float64
	DefQuantile map[float64]float64
}

type Collectors struct {
	mu          sync.Mutex
	collectors  map[string]Collector
	defBuckets  []float64
	defQuantile map[float64]float64
	port        int
	location    string
}

// NewCollectors creates a new Collectors using the default options.
func NewCollectors(opts *CollectorsOpts) (*Collectors, error) {
	return NewCollectorsFrom(DefaultCollectorsOpts)
}

// NewCollectorsFrom creates a new Collectors using the passed options.
func NewCollectorsFrom(opts CollectorsOpts) (*Collectors, error) {
	metrics := &Collectors{
		collectors:  make(map[string]Collector),
		port:        opts.Port,
		location:    opts.Location,
		defBuckets:  opts.DefBuckets,
		defQuantile: opts.DefQuantile,
	}
	return metrics, prometheus.Register(metrics)
}

//no use only for register
func (p *Collectors) Describe(c chan<- *prometheus.Desc) {
	prometheus.NewGauge(prometheus.GaugeOpts{Name: "Dummy", Help: "Dummy"}).Describe(c)
}

// Collect meets the collection interface and allows us to enforce our expiration
// logic to clean up ephemeral metrics if their value haven't been set for a
// duration exceeding our allowed expiration time.
func (p *Collectors) Collect(c chan<- prometheus.Metric) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.collectors {
		fmt.Println("test=counters=", k, v)
		v.Collect(c)
	}
}

type Collector interface {
	Collect(chan<- prometheus.Metric)
	Sample(v float64)
}
type counterCollector struct {
	prometheus.Counter
}

func (m *counterCollector) Collect(c chan<- prometheus.Metric) {
	m.Counter.Collect(c)
}

func (m *counterCollector) Sample(v float64) {
	m.Counter.Add(v)
}

type gaugeCollector struct {
	prometheus.Gauge
}

func (m *gaugeCollector) Collect(c chan<- prometheus.Metric) {
	m.Gauge.Collect(c)
}

func (m *gaugeCollector) Sample(v float64) {
	m.Gauge.Set(v)
}

type histogramCollector struct {
	prometheus.Histogram
}

func (m *histogramCollector) Collect(c chan<- prometheus.Metric) {
	m.Histogram.Collect(c)
}

func (m *histogramCollector) Sample(v float64) {
	m.Histogram.Observe(v)
}

type summaryCollector struct {
	prometheus.Summary
}

func (m *summaryCollector) Collect(c chan<- prometheus.Metric) {
	m.Summary.Collect(c)
}

func (m *summaryCollector) Sample(v float64) {
	m.Summary.Observe(v)
}

// CollectorsOpts is used to configure the Collectors
type CollectorOpts struct {
	Type     CollectorType
	Keys     []string
	Labels   []Label
	Buckets  []float64
	Quantile map[float64]float64
}

func (p *Collectors) CreateCollector(opts *CollectorOpts) (Collector, error) {
	key, hash := flattenKey(opts.Keys, opts.Labels)
	var m Collector
	var build func() Collector
	switch opts.Type {
	case Counter:
		build = func() Collector {
			return &counterCollector{prometheus.NewCounter(prometheus.CounterOpts{
				Name:        key,
				Help:        key,
				ConstLabels: prometheusLabels(opts.Labels),
			})}
		}
	case Gauge:
		build = func() Collector {
			return &gaugeCollector{prometheus.NewGauge(prometheus.GaugeOpts{
				Name:        key,
				Help:        key,
				ConstLabels: prometheusLabels(opts.Labels),
			})}
		}
	case Histogram:
		build = func() Collector {
			return &histogramCollector{prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:        key,
				Help:        key,
				ConstLabels: prometheusLabels(opts.Labels),
				Buckets:     opts.Buckets,
			})}
		}
	case Summary:
		build = func() Collector {
			return &summaryCollector{prometheus.NewSummary(prometheus.SummaryOpts{
				Name:        key,
				Help:        key,
				MaxAge:      10 * time.Second,
				ConstLabels: prometheusLabels(opts.Labels),
				Objectives:  opts.Quantile,
			})}
		}
	case Untyped:
		slog.Warnf("create invalid collector: %v", opts)
		return nil, fmt.Errorf("invalid collector type %d", opts.Type)
	}
	m = p.putCollectorIfAbsent(hash, build)
	return m, nil
}

func (p *Collectors) putCollectorIfAbsent(hash string, build func() Collector) Collector {
	g, ok := p.collectors[hash]
	if !ok {
		p.mu.Lock()
		defer p.mu.Unlock()
		g, ok = p.collectors[hash]
		if !ok {
			g = build()
			if g != nil {
				p.collectors[hash] = g
			}
		}
	}
	return g
}

func (p *Collectors) start() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(p)

	http.Handle(p.location, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>A Prometheus Exporter</title></head>
			<body>
			<h1>A Prometheus Exporter</h1>
			<p><a href='/metrics'>Collectors</a></p>
			</body>
			</html>`))
	})
	slog.Infof("Starting Server at http://localhost:%s%s", p.port, p.location)
	slog.Errorln(http.ListenAndServe(":"+strconv.Itoa(p.port), nil))
}
