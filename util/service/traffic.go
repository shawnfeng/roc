package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/scontext"
	"github.com/shawnfeng/sutil/slog/slog"
	"github.com/uber/jaeger-client-go"
	"net/http"
	"strings"
)

const TrafficLogID = "TRAFFIC"

func httpTrafficLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// NOTE: log before handling business logic
		logTrafficForHttpServer(r.Context())
		next.ServeHTTP(w, r)
	})
}

func trafficKVFromContext(ctx context.Context) (kv map[string]interface{}) {
	kv = map[string]interface{}{}

	kv["uid"], _ = scontext.GetUid(ctx)
	kv["group"] = scontext.GetGroup(ctx)

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return
	}

	if spanCtx, ok := span.Context().(jaeger.SpanContext); ok {
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
	logTrafficByKV(ctx, kv)
}

func serviceFromServPath(spath string) string {
	// NOTE: 若 sep 不为空, strings.Split 返回的字符串数组长度至少为 1
	parts := strings.Split(spath, "/")
	return parts[len(parts)-1]
}

func logTrafficForClientThrift(ctx context.Context, ct *ClientThrift, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv["srvid"] = si.Servid
	kv["sname"] = serviceFromServPath(ct.clientLookup.ServPath())
	logTrafficByKV(ctx, kv)
}

func logTrafficForClientGrpc(ctx context.Context, cg *ClientGrpc, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv["stype"] = si.Type
	kv["srvid"] = si.Servid
	kv["sname"] = serviceFromServPath(cg.clientLookup.ServPath())
	logTrafficByKV(ctx, kv)
}

func logTrafficByKV(ctx context.Context, kv map[string]interface{}) {
	bs, _ := json.Marshal(kv)
	slog.Infof(ctx, "%s %s", TrafficLogID, string(bs))
}
