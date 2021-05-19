package rocserv

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit/registry"
)

var (
	rateLimitRegistry registry.InterfaceRateLimitRegistry
)

const UNSPECIFIED_CALLER = "NULL"

// 获取接口限流的 registry 管理对象。
// thrift 服务，无法直接在 roc 里面统一搞定限流。暴露这个函数，以供 codegen 使用。
func GetInterfaceRateLimitRegistry() registry.InterfaceRateLimitRegistry {
	return rateLimitRegistry
}

// 从trace中获取baggage上游服务
func GetCallerFromBaggage(ctx context.Context) string {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return UNSPECIFIED_CALLER
	}
	caller := span.BaggageItem("ipalfish-roc-caller")
	return caller
}
