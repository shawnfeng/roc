package rocserv

import (
	"gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
)

const (
	namespacePalfish     = "palfish"
	namespaceAPM         = "apm"
	serverDurationSecond = "server_duration_second"
	serverRequestTotal   = "server_request_total"
	// client means that the duration is count from client side,
	//   which includes the network round-trip time and the server
	//   processing time.
	clientRequestTotal    = "client_request_total"
	clientRequestDuration = "client_request_duration"

	labelStatus = "status"

	apiType = "api"
	logType = "log"
	dbType  = "db"
)

var (
	buckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	// 目前只用作sla统计，后续通过修改标签作为所有微服务的耗时统计
	_metricRequestDuration = xprom.NewHistogram(&xprom.HistogramVecOpts{
		Namespace:  namespacePalfish,
		Name:       serverDurationSecond,
		Help:       "sla calc request duration in seconds.",
		Buckets:    buckets,
		LabelNames: []string{xprom.LabelServiceName, xprom.LabelServiceID, xprom.LabelInstance, xprom.LabelAPI, xprom.LabelSource, xprom.LabelType},
	})
	_metricRequestTotal = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespacePalfish,
		Name:       serverRequestTotal,
		Help:       "sla calc request total.",
		LabelNames: []string{xprom.LabelServiceName, xprom.LabelServiceID, xprom.LabelInstance, xprom.LabelAPI, xprom.LabelSource, xprom.LabelType, labelStatus},
	})

	// apm 打点
	_metricAPMRequestDuration = xprom.NewHistogram(&xprom.HistogramVecOpts{
		Namespace:  namespaceAPM,
		Name:       clientRequestDuration,
		Help:       "apm client side request duration in seconds",
		Buckets:    buckets,
		LabelNames: []string{xprom.LabelCallerService, xprom.LabelCalleeService, xprom.LabelCallerEndpoint, xprom.LabelCalleeEndpoint, xprom.LabelCallerServiceID},
	})

	_metricAPMRequestTotal = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespaceAPM,
		Name:       clientRequestTotal,
		Help:       "apm client side request total",
		LabelNames: []string{xprom.LabelCallerService, xprom.LabelCalleeService, xprom.LabelCallerEndpoint, xprom.LabelCalleeEndpoint, xprom.LabelCallerServiceID, xprom.LabelCallStatus},
	})

	_metricAPIRequestCount = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespacePalfish,
		Subsystem:  apiType,
		Name:       "request_count",
		Help:       "api request count",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelAPI},
	})

	_metricAPIRequestTime = xprom.NewHistogram(&xprom.HistogramVecOpts{
		Namespace:  namespacePalfish,
		Subsystem:  apiType,
		Name:       "request_duration",
		Buckets:    []float64{10, 50, 100, 200, 300, 500, 1000, 3000, 5000, 10000},
		Help:       "api request duration in millisecond",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelAPI},
	})

	// warn log count
	_metricLogCount = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespacePalfish,
		Subsystem:  logType,
		Name:       "request_count",
		Help:       "log count",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelType},
	})

	_metricDBRequestCount = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespacePalfish,
		Subsystem:  dbType,
		Name:       "request_count",
		Help:       "db request count",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelSource},
	})

	_metricDBRequestTime = xprom.NewHistogram(&xprom.HistogramVecOpts{
		Namespace:  namespacePalfish,
		Subsystem:  dbType,
		Name:       "request_duration",
		Buckets:    []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		Help:       "db request time",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelSource},
	})
)

func GetSlaDurationMetric() xmetric.Histogram {
	return _metricRequestDuration
}
func GetSlaRequestTotalMetric() xmetric.Counter {
	return _metricRequestTotal
}

// GetAPIRequestCountMetric export api request count metric
func GetAPIRequestCountMetric() xmetric.Counter {
	return _metricAPIRequestCount
}

// GetAPIRequestTimeMetric export api request time metric
func GetAPIRequestTimeMetric() xmetric.Histogram {
	return _metricAPIRequestTime
}

// GetLogCountMetric export log request count metric
func GetLogCountMetric() xmetric.Counter {
	return _metricLogCount
}

// GetDBRequestCountMetric export db request count metric
func GetDBRequestCountMetric() xmetric.Counter {
	return _metricDBRequestCount
}

// GetDBRequestTimeMetric export db request time metric
func GetDBRequestTimeMetric() xmetric.Histogram {
	return _metricDBRequestTime
}
