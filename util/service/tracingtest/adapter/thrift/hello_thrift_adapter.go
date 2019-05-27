package thrift

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shawnfeng/roc/util/service"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/thrift/gen-go/hello"
	"time"
)

var helloClient *rocserv.ClientThrift

func init() {
	etcds := []string{"http://localhost:2379"}
	clientLookup, _ := rocserv.NewClientLookup(etcds, "/tracing", "hello")

	cbfac := func(t thrift.TTransport, f thrift.TProtocolFactory) interface{} {
		return hello.NewHelloServiceClientFactory(t, f)
	}

	helloClient = rocserv.NewClientThrift(clientLookup, "thrift_hello", cbfac, 10)
}

func requestHello(haskkey string, timeout time.Duration, fn func(client *hello.HelloServiceClient) error) error {
	return helloClient.Rpc(haskkey, timeout, func(c interface{}) error {
		ct, ok := c.(*hello.HelloServiceClient)
		if ok {
			return fn(ct)
		} else {
			return fmt.Errorf("reflect client thrift error")
		}

	})
}

func SayHello(name string, ctx context.Context) (r string) {
	tctx := rocserv.GoContextToThriftContext(ctx)
	requestHello("haha", time.Millisecond*5000,
		func(c *hello.HelloServiceClient) (er error) {
			r, er = c.SayHello(name, tctx)
			return er
		})
	return
}
