package main

import (
	"fmt"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/service/grpc"
	"github.com/shawnfeng/roc/util/service/tracingtest/service/http"
	"github.com/shawnfeng/roc/util/service/tracingtest/service/thrift"
)

func main() {
	ps := map[string]rocserv.Processor{
		"grpc_hello": &grpc.HelloGrpc{},
		"thrift_hello": &thrift.HelloThrift{},
		"http_hello": &http.HelloHttp{},
	}

	etcds := []string{"http://localhost:2379"}
	noop := func(sb rocserv.ServBase) error {
		return nil
	}

	err := rocserv.Serve(etcds, "/tracing", noop, ps)
	if err != nil {
		fmt.Println("init thrift server error")
	}
}
