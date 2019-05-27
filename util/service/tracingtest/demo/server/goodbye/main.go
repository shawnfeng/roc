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
		"grpc_goodbye": &grpc.GoodbyeGrpc{},
		"thrift_goodbye": &thrift.GoodbyeThrift{},
		"http_goodbye": &http.GoodbyeHttp{},
	}

	etcds := []string{"http://localhost:2379"}
	noop := func(sb rocserv.ServBase) error {
		return nil
	}
	fmt.Println("yes")

	err := rocserv.Serve(etcds, "/tracing", noop, ps)
	if err != nil {
		fmt.Println("init thrift server error")
	}
}
