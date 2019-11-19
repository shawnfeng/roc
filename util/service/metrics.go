package rocserv

import (
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
)

const (
	namespacePalfish     = "palfish"
	serverDurationSecond = "server_duration_second"
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
)
