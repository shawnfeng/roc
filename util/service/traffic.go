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

const (
	TrafficLogID              = "TRAFFIC"
	TrafficLogKeyUID          = "uid"
	TrafficLogKeyGroup        = "group"
	TrafficLogKeyTraceID      = "tid"
	TrafficLogKeySpanID       = "sid"
	TrafficLogKeyParentSpanID = "pid"
	TrafficLogKeyOperation    = "op"
	TrafficLogKeyCaller       = "caller"
	TrafficLogKeyServerType   = "stype"
	TrafficLogKeyServerID     = "srvid"
	TrafficLogKeyServerName   = "sname"
)

func httpTrafficLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// NOTE: log before handling business logic
		logTrafficForHttpServer(r.Context())
		next.ServeHTTP(w, r)
	})
}

func trafficKVFromContext(ctx context.Context) (kv map[string]interface{}) {
	kv = map[string]interface{}{}

	kv[TrafficLogKeyUID], _ = scontext.GetUid(ctx)
	kv[TrafficLogKeyGroup] = scontext.GetControlRouteGroupWithDefault(ctx, scontext.DefaultGroup)

	if callerName, ok := scontext.GetControlCallerServerName(ctx); ok {
		kv[TrafficLogKeyCaller] = callerName
	}

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return
	}

	if jaegerSpan, ok := span.(*jaeger.Span); ok {
		jaegerSpanCtx, ok := jaegerSpan.Context().(jaeger.SpanContext)
		if !ok {
			return
		}

		kv[TrafficLogKeyOperation] = jaegerSpan.OperationName()
		kv[TrafficLogKeyTraceID] = fmt.Sprint(jaegerSpanCtx.TraceID())
		kv[TrafficLogKeySpanID] = fmt.Sprint(jaegerSpanCtx.SpanID())
		kv[TrafficLogKeyParentSpanID] = fmt.Sprint(jaegerSpanCtx.ParentID())
	}
	return
}

func logTrafficForHttpServer(ctx context.Context) {
	kv := make(map[string]interface{})
	kv[TrafficLogKeyServerType] = "http"
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

func logTrafficByKV(ctx context.Context, kv map[string]interface{}) {
	bs, _ := json.Marshal(kv)
	slog.Infof(ctx, "%s\t%s", TrafficLogID, string(bs))
}
