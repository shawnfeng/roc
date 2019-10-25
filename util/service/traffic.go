package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/scontext"
	"github.com/shawnfeng/sutil/slog"
	"github.com/uber/jaeger-client-go"
	"net/http"
)

const TrafficLogID = "TRAFFIC"

func httpTrafficLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		// NOTE: log after handling business logic
		logTrafficForHttpServer(r.Context())
	})
}

func trafficKVFromContext(ctx context.Context) (kv map[string]interface{}) {
	kv = map[string]interface{}{}

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return
	}

	if spanCtx, ok := span.Context().(jaeger.SpanContext); ok {
		kv["group"] = scontext.GetGroup(ctx)
		kv["tid"] = fmt.Sprint(spanCtx.TraceID())
		kv["sid"] = fmt.Sprint(spanCtx.SpanID())
		kv["pid"] = fmt.Sprint(spanCtx.ParentID())
	}
	return
}

func logTrafficForHttpServer(ctx context.Context) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}
	logTrafficByKV(kv)
}

func logTrafficForClientThrift(ctx context.Context, ct *ClientThrift, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv["srvid"] = si.Servid
	kv["spath"] = ct.clientLookup.ServPath()
	logTrafficByKV(kv)
}

func logTrafficForClientGrpc(ctx context.Context, cg *ClientGrpc, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv["srvid"] = si.Servid
	kv["spath"] = cg.clientLookup.ServPath()
	logTrafficByKV(kv)
}

func logTrafficByKV(kv map[string]interface{}) {
	bs, _ := json.Marshal(kv)
	slog.Infof("%s %s", TrafficLogID, string(bs))
}
