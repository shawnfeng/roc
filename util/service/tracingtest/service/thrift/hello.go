package thrift

import (
	"code.ibanyu.com/server/go/util.git/idl/gen-go/util/thriftutil"
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/thrift/gen-go/hello"
)

type HelloThriftServiceImpl2 struct{}

func (h *HelloThriftServiceImpl2) SayHello(name string, ctx context.Context) (r string, err error) {
	fun := "HelloThriftServiceImpl.SayHello-->"
	fmt.Printf("%s called with name:%s\n", fun, name)

	span := opentracing.SpanFromContext(ctx)
	span.SetTag("by", fun)

	//fmt.Println(grpc.SayGoodbye(ctx, &tracingtest.SayGoodbyeRequest{Name: name}))
	//fmt.Println(thrift.SayGoodbye(name, ctx))
	return "Hello, " + name, nil
}

type HelloThriftServiceImpl struct {
	impl *HelloThriftServiceImpl2
}

func (h *HelloThriftServiceImpl) SayHello(name string, tctx *thriftutil.Context) (r string, err error) {
	ctx := rocserv.ThriftContextToGoContext(tctx, "Greet")
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		defer span.Finish()
	}
	return h.impl.SayHello(name, ctx)
}

type HelloThrift struct {}

func (m *HelloThrift) Init() error {
	fun := "HelloThrift.Init-->"
	fmt.Printf("%s called\n", fun)
	return nil
}

func (m *HelloThrift) Driver() (string, interface{}) {
	fun := "HelloThrift.Driver-->"
	fmt.Printf("%s called\n", fun)

	handler := &HelloThriftServiceImpl{
		impl: &HelloThriftServiceImpl2{},
	}
	processor := hello.NewHelloServiceProcessor(handler)

	return ":", processor
}




