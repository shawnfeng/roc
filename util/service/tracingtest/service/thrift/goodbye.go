package thrift

import (
	"code.ibanyu.com/server/go/util.git/idl/gen-go/util/thriftutil"
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/thrift/gen-go/goodbye"
)

type GoodbyeThriftServieImpl2 struct{}

func (h *GoodbyeThriftServieImpl2) SayGoodbye(name string, ctx context.Context) (r string, err error) {
	fun := "Goodbyethriftserviceimpl.SayGoodbye-->"
	fmt.Printf("%s called with name:%s\n", fun, name)

	span := opentracing.SpanFromContext(ctx)
	span.SetTag("by", fun)

	return "Goodbye, " + name, nil
}

type GoodbyeThriftServiceImpl struct {
	impl *GoodbyeThriftServieImpl2
}

func (h *GoodbyeThriftServiceImpl) SayGoodbye(name string, tctx *thriftutil.Context) (r string, err error) {
	ctx := rocserv.ThriftContextToGoContext(tctx, "SayGoodbye")
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		defer span.Finish()
	}
	return h.impl.SayGoodbye(name, ctx)
}

type GoodbyeThrift struct{}

func (m *GoodbyeThrift) Init() error {
	fun := "GoodbyeThrift.Init-->"
	fmt.Printf("%s called\n", fun)
	return nil
}

func (m *GoodbyeThrift) Driver() (string, interface{}) {
	fun := "GoodbyeThrift.Driver-->"
	fmt.Printf("%s called\n", fun)

	handler := &GoodbyeThriftServiceImpl{
		impl: &GoodbyeThriftServieImpl2{},
	}
	processor := goodbye.NewGoodbyeServiceProcessor(handler)

	return ":", processor
}
