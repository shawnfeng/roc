package rocserv

import (
	"gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
)

const (
	namespacePalfish     = "palfish"
	serverDurationSecond = "server_duration_second"
	serverRequstTotal    = "server_request_total"
	labelStatus          = "status"

	namespace = "biz"
	apiType = "api"
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
		Name:       serverRequstTotal,
		Help:       "sla calc request total.",
		LabelNames: []string{xprom.LabelServiceName, xprom.LabelServiceID, xprom.LabelInstance, xprom.LabelAPI, xprom.LabelSource, xprom.LabelType, labelStatus},
	})

	// monitor系统当前打点元信息, 同server/go/util/servbase/monitor
	_metricAPIRequestCount = xprom.NewCounter(&xprom.CounterVecOpts{
		Namespace:  namespace,
		Subsystem:  apiType,
		Name:       "request_count",
		Help:       "api request count",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelAPI},
	})

	_metricAPIRequestTime = xprom.NewHistogram(&xprom.HistogramVecOpts{
		Namespace:  namespace,
		Subsystem:  apiType,
		Name:       "request_duration",
		Buckets:    buckets,
		Help:       "api request duration in millisecond",
		LabelNames: []string{xprom.LabelGroupName, xprom.LabelServiceName, xprom.LabelAPI},
	})
)

func GetSlaDurationMetric() xmetric.Histogram {
	return _metricRequestDuration
}
func GetSlaRequestTotalMetric() xmetric.Counter {
	return _metricRequestTotal
}

// GetAPIRequestCountMetric 解决server/go/util/servbase/monitor与roc循环引用及重名metric的问题
func GetAPIRequestCountMetric() xmetric.Counter{
	return _metricAPIRequestCount
}

// GetAPIRequestTimeMetric 解决server/go/util/servbase/monitor与roc循环引用及重名metric的问题
func GetAPIRequestTimeMetric() xmetric.Histogram{
	return _metricAPIRequestTime
}
