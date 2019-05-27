package main

import (
	"context"
	"fmt"
	"github.com/shawnfeng/roc/util/service"
	grpcAdapter "github.com/shawnfeng/roc/util/service/tracingtest/adapter/grpc"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/hello"
	"time"
)

func main() {
	_, closer := rocserv.InitJaeger("gggservice")
	defer closer.Close()

	//span := tracer.StartSpan("op1")
	//span.SetTag("by", "hello-grpc-client")
	//defer span.Finish()

	ctx := context.Background()
	//ctx = opentracing.ContextWithSpan(ctx, span)

	time.Sleep(2*time.Second)
	fmt.Println(grpcAdapter.SayHello(ctx, &tracingtest.SayHelloRequest{
		Name: "zhenghe",
	}))
}
