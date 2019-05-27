package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/hello"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

var helloClient *rocserv.ClientGrpc

func init() {
	etcds := []string{"http://localhost:2379"}
	clientLookup, _ := rocserv.NewGrpcClientLookup(etcds, "/tracing", "hello")

	fac := func(cc *grpc.ClientConn) interface{} {
		return tracingtest.NewHelloClient(cc)
	}

	helloClient = rocserv.NewClientGrpc(clientLookup, "grpc_hello", 10, fac)
}

func requestHello(haskkey string, fn func(client tracingtest.HelloClient) error) error {
	return helloClient.Rpc(haskkey, func(c interface{}) error {
		ct, ok := c.(tracingtest.HelloClient)
		if ok {
			return fn(ct)
		} else {
			return errors.New("")
		}
	})
}

func SayHello(ctx context.Context, req *tracingtest.SayHelloRequest) (r *tracingtest.SayHelloReply) {
	err := requestHello(strconv.Itoa(rand.Int()), func(c tracingtest.HelloClient) (er error) {
		ctx, cancel := context.WithTimeout(ctx, time.Second*100)
		defer cancel()
		r, er = c.SayHello(ctx, req)
		if er != nil {
			fmt.Printf("could not greet: %v", er)
			return er
		}
		return nil
	})

	if err != nil {
		return &tracingtest.SayHelloReply{
			Message: err.Error(),
		}
	}
	return
}
//
