package rocserv

import (
	"code.ibanyu.com/server/go/util.git/idl/gen-go/util/thriftutil"
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/shawnfeng/sutil/slog"
	"github.com/uber/jaeger-client-go/config"
	"io"
	"net/http"
)

func InitJaeger(serviceName string) (opentracing.Tracer, io.Closer) {
	fun := "InitJaeger-->"
	cfg := config.Configuration{
		ServiceName:         serviceName,
		Sampler:             &config.SamplerConfig{
			Type:                    "const",
			Param:                   1,
		},
		Reporter:            &config.ReporterConfig{
			LogSpans:            true,
			CollectorEndpoint:   "http://10.111.209.188:20352/api/traces",
		},
	}

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic("init jaeger failed")
	}

	opentracing.SetGlobalTracer(tracer)

	slog.Infof("%s succeed service:%s", fun, serviceName)
	return tracer, closer
}

func ThriftContextToGoContext(tctx *thriftutil.Context, opName string) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "Head", tctx.Head)

	tracer := opentracing.GlobalTracer()
	spanCtx, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(tctx.Spanctx))
	var span opentracing.Span
	if err == nil {
		span = tracer.StartSpan(opName, ext.RPCServerOption(spanCtx))
	} else {
		span = tracer.StartSpan(opName)
	}
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx
}

func GoContextToThriftContext(ctx context.Context) *thriftutil.Context {
	var head *thriftutil.Head
	head, ok := ctx.Value("Head").(*thriftutil.Head)
	if !ok {
		head = thriftutil.NewHead()
	}

	carrier := opentracing.TextMapCarrier(make(map[string]string))
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		opentracing.GlobalTracer().Inject(
			span.Context(),
			opentracing.TextMap,
			carrier)
	}

	tctx := &thriftutil.Context{
		Head:    head,
		Spanctx: carrier,
	}
	return tctx
}

func TraceHttpRequest(ctx context.Context, r *http.Request) {
	span := opentracing.SpanFromContext(ctx)
	opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(r.Header))
}