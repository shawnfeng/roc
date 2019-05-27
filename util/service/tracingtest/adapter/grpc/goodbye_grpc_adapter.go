package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/goodbye"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

var goodbyeClient *rocserv.ClientGrpc

func init() {
	etcds := []string{"http://localhost:2379"}
	clientLookup, _ := rocserv.NewGrpcClientLookup(etcds, "/tracing", "goodbye")

	fac := func(cc *grpc.ClientConn) interface{} {
		return tracingtest.NewGoodbyeClient(cc)
	}

	goodbyeClient = rocserv.NewClientGrpc(clientLookup, "grpc_goodbye", 10, fac)
}

func requestGoodbye(haskkey string, fn func(client tracingtest.GoodbyeClient) error) error {
	return goodbyeClient.Rpc(haskkey, func(c interface{}) error {
		ct, ok := c.(tracingtest.GoodbyeClient)
		if ok {
			return fn(ct)
		} else {
			return errors.New("")
		}
	})
}

func SayGoodbye(ctx context.Context, req *tracingtest.SayGoodbyeRequest) (r *tracingtest.SayGoodbyeReply) {
	err := requestGoodbye(strconv.Itoa(rand.Int()), func(c tracingtest.GoodbyeClient) (er error) {
		ctx, cancel := context.WithTimeout(ctx, time.Second*100)
		defer cancel()
		r, er = c.SayGoodbye(ctx, req)
		if er != nil {
			fmt.Printf("could not goodbye: %v", er)
			return er
		}
		return nil
	})

	if err != nil {
		return &tracingtest.SayGoodbyeReply{
			Message: err.Error(),
		}
	}
	return
}

