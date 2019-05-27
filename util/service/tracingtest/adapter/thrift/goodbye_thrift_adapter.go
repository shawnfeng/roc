package thrift

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/thrift/gen-go/goodbye"
	"time"
)

var goodbyeClient *rocserv.ClientThrift

func init() {
	etcds := []string{"http://localhost:2379"}
	clientLookup, _ := rocserv.NewClientLookup(etcds, "/tracing", "goodbye")

	cbfac := func(t thrift.TTransport, f thrift.TProtocolFactory) interface{} {
		return goodbye.NewGoodbyeServiceClientFactory(t, f)
	}

	goodbyeClient = rocserv.NewClientThrift(clientLookup, "thrift_goodbye", cbfac, 10)
}

func requestGoodbye(haskkey string, timeout time.Duration, fn func(client *goodbye.GoodbyeServiceClient) error) error {
	return goodbyeClient.Rpc(haskkey, timeout, func(c interface{}) error {
		ct, ok := c.(*goodbye.GoodbyeServiceClient)
		if ok {
			return fn(ct)
		} else {
			return fmt.Errorf("reflect client thrift error")
		}

	})
}

func SayGoodbye(name string, ctx context.Context) (r string) {
	tctx := rocserv.GoContextToThriftContext(ctx)
	err := requestGoodbye("haha", time.Millisecond*5000,
		func(c *goodbye.GoodbyeServiceClient) (er error) {
			r, er = c.SayGoodbye(name, tctx)
			return er
		})
	if err != nil {
		fmt.Println(err)
	}
	return
}
