package rocserv

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shawnfeng/sutil/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"regexp"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	DefaultMetricsOpts = &MetricsOpts{
		Port:        0,
		Location:    "/metrics",
		DefBuckets:  []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		DefQuantile: map[float64]float64{.25: .01, .5: .01, .75: .01, .9: .01, .95: .001, .99: .001},
	}
	MetricsInstance *Metrics
	state           = 0
)

const (
	Name_space_palfish          = "palfish"
	Name_server_req_total       = "server_request_total"
	Name_server_duration_second = "server_duration_second"
	Label_instance              = "instance"
	Label_servname              = "servname"
	Label_servid                = "servid"
	Label_api                   = "api"
	Label_type                  = "type"
	Label_source                = "source"
	Label_status                = "status"
	Status_succ                 = 1
	Status_fail                 = 0
)

// MetricsOpts is used to configure the Metrics
type MetricsOpts struct {
	Port        int32
	Location    string
	DefBuckets  []float64
	DefQuantile map[float64]float64
}

type Metrics struct {
	mu          sync.Mutex
	counters    map[string]prometheus.Counter
	gauges      map[string]prometheus.Gauge
	historams   map[string]prometheus.Histogram
	summaries   map[string]prometheus.Summary
	defBuckets  []float64
	defQuantile map[float64]float64
	port        int32
	location    string
}

// NewMetrics creates a new Metrics using the default options.
func NewMetrics() (*Metrics, error) {
	return NewMetricsFrom(DefaultMetricsOpts)
}

// NewMetricsFrom creates a new Metrics using the passed options.
func NewMetricsFrom(opts *MetricsOpts) (*Metrics, error) {
	metrics := &Metrics{
		counters:    make(map[string]prometheus.Counter),
		gauges:      make(map[string]prometheus.Gauge),
		historams:   make(map[string]prometheus.Histogram),
		summaries:   make(map[string]prometheus.Summary),
		port:        opts.Port,
		location:    opts.Location,
		defBuckets:  opts.DefBuckets,
		defQuantile: opts.DefQuantile,
	}
	return metrics, prometheus.Register(metrics)
}

//no use only for register
func (p *Metrics) Describe(c chan<- *prometheus.Desc) {
	prometheus.NewGauge(prometheus.GaugeOpts{Name: "Dummy", Help: "Dummy"}).Describe(c)
}

// Collect meets the collection interface and allows us to enforce our expiration
// logic to clean up ephemeral metrics if their value haven't been set for a
// duration exceeding our allowed expiration time.
func (p *Metrics) Collect(c chan<- prometheus.Metric) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.counters {
		v.Collect(c)
	}
	for _, v := range p.gauges {
		v.Collect(c)
	}
	for _, v := range p.historams {
		v.Collect(c)
	}
	for _, v := range p.summaries {
		v.Collect(c)
	}

}

type Label struct {
	Name  string
	Value string
}

var forbiddenChars = regexp.MustCompile("[ .=\\-/]")

func flattenKey(namekeys []string, labels []Label) (string, string) {
	key := strings.Join(namekeys, "_")
	key = SafePromethuesValue(key)

	hash := key
	for _, label := range labels {
		label.Name = SafePromethuesValue(label.Name)
		label.Value = SafePromethuesValue(label.Value)
		hash += fmt.Sprintf(";%s=%s", label.Name, label.Value)
	}

	return key, hash
}
func SafePromethuesValue(v string) string {
	return forbiddenChars.ReplaceAllString(v, "_")
}

func prometheusLabels(labels []Label) prometheus.Labels {
	l := make(prometheus.Labels)
	for _, label := range labels {
		l[label.Name] = label.Value
	}
	return l
}

func (p *Metrics) CreateCounter(namekeys []string, labels []Label) prometheus.Counter {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Counter
	var ok bool
	p.putMetricIfAbsent(func() bool {
		m, ok = p.counters[hash]
		return ok
	}, func() {
		m = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.counters[hash] = m
	})
	return m
}
func (p *Metrics) CreateGauge(namekeys []string, labels []Label) prometheus.Gauge {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Gauge
	var ok bool
	p.putMetricIfAbsent(func() bool {
		m, ok = p.gauges[hash]
		return ok
	}, func() {
		m = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.gauges[hash] = m
	})
	return m
}
func (p *Metrics) CreateHistogram(namekeys []string, labels []Label, buckets []float64) prometheus.Histogram {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Histogram
	var ok bool
	p.putMetricIfAbsent(func() bool {
		m, ok = p.historams[hash]
		return ok
	}, func() {
		if buckets == nil || len(buckets) == 0 {
			buckets = DefaultMetricsOpts.DefBuckets
		}
		m = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
			Buckets:     buckets,
		})
		p.historams[hash] = m
	})
	return m
}
func (p *Metrics) CreateSummary(namekeys []string, labels []Label, quantile map[float64]float64) prometheus.Summary {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Summary
	var ok bool
	p.putMetricIfAbsent(func() bool {
		m, ok = p.summaries[hash]
		return ok
	}, func() {
		if quantile == nil || len(quantile) == 0 {
			quantile = DefaultMetricsOpts.DefQuantile
		}
		m = prometheus.NewSummary(prometheus.SummaryOpts{
			Name:        key,
			Help:        key,
			MaxAge:      10 * time.Second,
			ConstLabels: prometheusLabels(labels),
			Objectives:  quantile,
		})
		p.summaries[hash] = m
	})
	return m
}
func (p *Metrics) putMetricIfAbsent(isExist func() bool, put func()) {
	ok := isExist()
	if !ok {
		p.mu.Lock()
		defer p.mu.Unlock()
		ok := isExist()
		if !ok {
			put()
		}
	}
}

func (p *Metrics) SetGaugeCreateIfAbsent(namekeys []string, val float64, labels []Label) {
	key, hash := flattenKey(namekeys, labels)
	g, ok := p.gauges[hash]
	if !ok {
		g = p.CreateGauge(namekeys, labels)
	}
	if g != nil {
		g.Set(val)
	} else {
		slog.Warnf("set gauge fail of no metric:%s,%s,%v", key, hash, val)
	}
}

func (p *Metrics) AddHistoramSampleCreateIfAbsent(namekeys []string, val float64, labels []Label, buckets []float64) {
	key, hash := flattenKey(namekeys, labels)
	g, ok := p.historams[hash]
	if !ok {
		g = p.CreateHistogram(namekeys, labels, buckets)
	}
	if g != nil {
		g.Observe(val)
	} else {
		slog.Warnf("set historam fail of no metric:%s,%s,%v", key, hash, val)
	}
}

func (p *Metrics) AddSummarySampleCreateIfAbsent(namekeys []string, val float64, labels []Label, quantile map[float64]float64) {

	key, hash := flattenKey(namekeys, labels)
	g, ok := p.summaries[hash]
	if !ok {
		g = p.CreateSummary(namekeys, labels, quantile)
	}
	if g != nil {
		g.Observe(val)
	} else {
		slog.Warnf("set summary fail of no metric:%s,%s,%v", key, hash, val)
	}
}

func (p *Metrics) IncrCounterCreateIfAbsent(namekeys []string, val float64, labels []Label) {
	key, hash := flattenKey(namekeys, labels)
	g, ok := p.counters[hash]
	if !ok {
		g = p.CreateCounter(namekeys, labels)
	}
	if g != nil {
		g.Add(val)
	} else {
		slog.Warnf("set counter fail of no metric:%s,%s,%v", key, hash, val)
	}
}
func IsMetricsInited() bool {
	return state == 1
}
func (p *Metrics) Init() error {
	slog.Infof("init metric instance")
	MetricsInstance = p
	state = 1
	return nil
}

func (p *Metrics) Driver() (string, interface{}) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(p)

	handlerFor := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	router := httprouter.New()
	router.Handler("GET", p.location, handlerFor)
	//router.Handler("POST", p.location,handlerFor)
	router.HandlerFunc("GET", "/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>A Prometheus Exporter</title></head>
			<body>
			<h1>A Prometheus Exporter</h1>
			<p><a href='/metrics'>Metrics</a></p>
			</body>
			</html>`))
	})
	return getPort(), router
}
func getPort() string {
	p := 22333
	for {
		s := "127.0.0.1:" + strconv.Itoa(p)
		_, err := net.Dial("tcp", s)
		fmt.Println("dial======", s, err)
		if err != nil {
			return s
		}
		p++
	}

}
