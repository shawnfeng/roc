package rocserv

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shawnfeng/sutil/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"regexp"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	DefaultMetricsOpts = &MetricsOpts{
		DefBuckets:  []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		DefQuantile: map[float64]float64{.25: .01, .5: .01, .75: .01, .9: .01, .95: .001, .99: .001},
	}
	DefaultMetrics = newMetrics()
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
	DefBuckets  []float64
	DefQuantile map[float64]float64
}

type Metrics struct {
	mucout      sync.RWMutex
	mugau       sync.RWMutex
	muhist      sync.RWMutex
	musumm      sync.RWMutex
	counters    map[string]prometheus.Counter
	gauges      map[string]prometheus.Gauge
	historams   map[string]prometheus.Histogram
	summaries   map[string]prometheus.Summary
	defBuckets  []float64
	defQuantile map[float64]float64
	registry    *prometheus.Registry
}

// NewMetrics creates a new Metrics using the default options.
func newMetrics() *Metrics {
	return newMetricsFrom(DefaultMetricsOpts)
}

// NewMetricsFrom creates a new Metrics using the passed options.
func newMetricsFrom(opts *MetricsOpts) *Metrics {
	metrics := &Metrics{
		counters:    make(map[string]prometheus.Counter, 512),
		gauges:      make(map[string]prometheus.Gauge, 512),
		historams:   make(map[string]prometheus.Histogram, 512),
		summaries:   make(map[string]prometheus.Summary, 512),
		defBuckets:  opts.DefBuckets,
		defQuantile: opts.DefQuantile,
		registry:    prometheus.NewRegistry(),
	}
	return metrics
}

// register collector
func (p *Metrics) regist(c prometheus.Collector) {
	err := p.registry.Register(c)
	if err != nil {
		slog.Warnf("sla register collector error: collector:%v,err:%v", c, err)
	}
}

//no use only for register
func (p *Metrics) Describe(c chan<- *prometheus.Desc) {
	prometheus.NewGauge(prometheus.GaugeOpts{Name: "Dummy", Help: "Dummy"}).Describe(c)
}

// Collect meets the collection interface and allows us to enforce our expiration
// logic to clean up ephemeral metrics if their value haven't been set for a
// duration exceeding our allowed expiration time.
func (p *Metrics) Collect(c chan<- prometheus.Metric) {
	//rlockCollect(c,&p.mucout,p.counters)
	//rlockCollect(c,&p.mugau,p.gauges)
	//rlockCollect(c,&p.muhist,p.historams)
	//rlockCollect(c,&p.musumm,p.summaries)
	p.rlockCollectCounter(c)
	p.rlockCollectGauge(c)
	p.rlockCollectHistorams(c)
	p.rlockCollectSummaries(c)
}
func rlockCollect(c chan<- prometheus.Metric, rw *sync.RWMutex, collmap map[string]prometheus.Collector) {
	rw.RLock()
	defer rw.RUnlock()
	for _, v := range collmap {
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
	p.putMetricIfAbsentWithWLock(&p.mucout, func() bool {
		m, ok = p.counters[hash]
		return ok
	}, func() {
		m = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.regist(m)
		p.counters[hash] = m
	})
	return m
}
func (p *Metrics) CreateGauge(namekeys []string, labels []Label) prometheus.Gauge {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Gauge
	var ok bool
	p.putMetricIfAbsentWithWLock(&p.mugau, func() bool {
		m, ok = p.gauges[hash]
		return ok
	}, func() {
		m = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.regist(m)
		p.gauges[hash] = m
	})
	return m
}
func (p *Metrics) CreateHistogram(namekeys []string, labels []Label, buckets []float64) prometheus.Histogram {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Histogram
	var ok bool
	p.putMetricIfAbsentWithWLock(&p.muhist, func() bool {
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
		p.regist(m)
		p.historams[hash] = m
	})
	return m
}
func (p *Metrics) CreateSummary(namekeys []string, labels []Label, quantile map[float64]float64) prometheus.Summary {
	key, hash := flattenKey(namekeys, labels)
	var m prometheus.Summary
	var ok bool
	p.putMetricIfAbsentWithWLock(&p.musumm, func() bool {
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
		p.regist(m)
		p.summaries[hash] = m
	})
	return m
}
func (p *Metrics) putMetricIfAbsentWithWLock(rw *sync.RWMutex, isExist func() bool, put func()) {
	rw.Lock()
	defer rw.Unlock()
	ok := isExist()
	if !ok {
		put()
	}
}

func (p *Metrics) SetGaugeCreateIfAbsent(namekeys []string, val float64, labels []Label) {
	key, hash := flattenKey(namekeys, labels)
	g, ok := p.rlockGetGauge(hash)
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
	g, ok := p.rlockGetHistogram(hash)
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
	g, ok := p.rlockGetSummary(hash)
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
	g, ok := p.rlockGetCounter(hash)
	if !ok {
		g = p.CreateCounter(namekeys, labels)
	}
	if g != nil {
		g.Add(val)
	} else {
		slog.Warnf("set counter fail of no metric:%s,%s,%v", key, hash, val)
	}
}
func (p *Metrics) rlockGetCounter(hash string) (prometheus.Counter, bool) {
	p.mucout.RLock()
	defer p.mucout.RUnlock()
	g, ok := p.counters[hash]
	return g, ok
}
func (p *Metrics) rlockGetGauge(hash string) (prometheus.Gauge, bool) {
	p.mugau.RLock()
	defer p.mugau.RUnlock()
	g, ok := p.gauges[hash]
	return g, ok
}
func (p *Metrics) rlockGetHistogram(hash string) (prometheus.Histogram, bool) {
	p.muhist.RLock()
	defer p.muhist.RUnlock()
	g, ok := p.historams[hash]
	return g, ok
}
func (p *Metrics) rlockGetSummary(hash string) (prometheus.Summary, bool) {
	p.musumm.RLock()
	defer p.musumm.RUnlock()
	g, ok := p.historams[hash]
	return g, ok
}
func (p *Metrics) Exportor() http.Handler {
	handlerFor := promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{
		ErrorLog:      NewPromErrorLog(),
		ErrorHandling: promhttp.ContinueOnError,
	})
	return handlerFor
}

type promErrorLog struct {
}

func (m *promErrorLog) Println(v ...interface{}) {
	slog.Warnln("promethues collect fail", v...)
}

func NewPromErrorLog() *promErrorLog {
	return &promErrorLog{}
}

func (p *Metrics) rlockCollectCounter(c chan<- prometheus.Metric) {
	p.mucout.RLock()
	defer p.mucout.RUnlock()
	for _, v := range p.counters {
		v.Collect(c)
	}
}

func (p *Metrics) rlockCollectGauge(c chan<- prometheus.Metric) {
	p.mugau.RLock()
	defer p.mugau.RUnlock()
	for _, v := range p.gauges {
		v.Collect(c)
	}
}

func (p *Metrics) rlockCollectHistorams(c chan<- prometheus.Metric) {
	p.muhist.RLock()
	defer p.muhist.RUnlock()
	for _, v := range p.historams {
		v.Collect(c)
	}
}

func (p *Metrics) rlockCollectSummaries(c chan<- prometheus.Metric) {
	p.musumm.RLock()
	defer p.musumm.RUnlock()
	for _, v := range p.summaries {
		v.Collect(c)
	}
}
