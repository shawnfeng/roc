package grpc

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/adapter/grpc"
	"github.com/shawnfeng/roc/util/service/tracingtest/adapter/thrift"
	tracingtest2 "github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/goodbye"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/hello"
)

type HelloGrpcServiceImpl struct {}

func (h *HelloGrpcServiceImpl) SayHello(ctx context.Context, in *tracingtest.SayHelloRequest) (*tracingtest.SayHelloReply, error) {
	fun := "HelloGrpcServiceImpl.SayHello-->"
	fmt.Printf("%s called", fun)

	span := opentracing.SpanFromContext(ctx)
	span.SetTag("by", fun)

	thrift.SayGoodbye(in.Name + "-thrift", ctx)
	grpc.SayGoodbye(ctx, &tracingtest2.SayGoodbyeRequest{Name: in.Name+"-grpc"})

	return &tracingtest.SayHelloReply{
		Message: "Hello, " + in.Name,
	}, nil
}

type HelloGrpc struct {}

func (h *HelloGrpc) Init() error {
	fun := "HelloGrpc.Init-->"
	fmt.Printf("%s called\n", fun)
	return nil
}

func (h *HelloGrpc) Driver() (string, interface{}) {
	fun := "HelloGrpc.Driver-->"
	fmt.Printf("%s called\n", fun)

	helloServer := &HelloGrpcServiceImpl{}
	s := rocserv.NewGrpcServer()
	tracingtest.RegisterHelloServer(s.Server, helloServer)
	return ":", s
}