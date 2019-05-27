package main

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/adapter/thrift"
	"time"
)

func main() {
	tracer, closer := rocserv.InitJaeger("tttservice")
	defer closer.Close()

	span := tracer.StartSpan("testOp1")
	span.SetTag("by", "hello-thrift-client")
	defer span.Finish()

	textCarrier := opentracing.TextMapCarrier(make(map[string]string))
	tracer.Inject(span.Context(), opentracing.TextMap, textCarrier)

	time.Sleep(5*time.Second)
	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	r := thrift.SayHello("zhenghe", ctx)
	fmt.Println(r)
}