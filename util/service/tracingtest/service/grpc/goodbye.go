package grpc

import (
	"context"
	"fmt"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/goodbye"
)

type GoodbyeGrpcServiceImpl struct {}

func (h *GoodbyeGrpcServiceImpl) SayGoodbye(ctx context.Context, in *tracingtest.SayGoodbyeRequest) (*tracingtest.SayGoodbyeReply, error) {
	fun := "GoodbyeGrpcServiceImpl.SayGoodbye-->"
	fmt.Printf("%s called", fun)

	return &tracingtest.SayGoodbyeReply{
		Message: "Goodbye, " + in.Name,
	}, nil
}

type GoodbyeGrpc struct {}

func (h *GoodbyeGrpc) Init() error {
	fun := "GoodbyeGrpc.Init-->"
	fmt.Printf("%s called\n", fun)
	return nil
}

func (h *GoodbyeGrpc) Driver() (string, interface{}) {
	fun := "GoodbyeGrpc.Driver-->"
	fmt.Printf("%s called\n", fun)

	goodbyeServer := &GoodbyeGrpcServiceImpl{}
	s := rocserv.NewGrpcServer()
	tracingtest.RegisterGoodbyeServer(s.Server, goodbyeServer)
	return ":", s
}