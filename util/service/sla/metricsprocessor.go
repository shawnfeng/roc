package rocserv

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/shawnfeng/sutil/slog"
	"net"
	"net/http"
	"strconv"
)

var (
	default_metric_location = "/metrics"
)

type Metricsprocessor struct {
	*Metrics
}

func NewMetricsprocessor() *Metricsprocessor {
	return &Metricsprocessor{DefaultMetrics}
}
func (p *Metricsprocessor) Init() error {
	slog.Infof("init metric instance")
	return nil
}

func (p *Metricsprocessor) Driver() (string, interface{}) {

	handlerFor := p.Metrics.Exportor()
	router := httprouter.New()
	router.Handler("GET", default_metric_location, handlerFor)
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
	return p.getListenerAddress(), router
}
func (p *Metricsprocessor) getListenerAddress() string {
	//return ""
	port := 22333
	for {
		s := "127.0.0.1:" + strconv.Itoa(port)
		_, err := net.Dial("tcp", s)
		fmt.Println("dial======", s, err)
		if err != nil {
			return s
		}
		port++
	}
}
