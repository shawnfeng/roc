// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/shawnfeng/sutil/slog"

	"github.com/shawnfeng/roc/util/service/test/idl/gen-go/demo/rpc"
)

func TestClient(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}

	cli, err := NewClientLookup(etcds, "niubi/fuck")

	slog.Infof("Test client:%s err:%v", cli, err)

	if err != nil {
		t.Errorf("create err:%s", err)
		return
	}
	time.Sleep(time.Second * 2)

	allserv := cli.GetAllServAddr()
	slog.Infoln("ALL", allserv)


	s := cli.GetServAddr("noexit", "key")
	if s != nil {
		t.Errorf("get err")
	}

	s = cli.GetServAddr("test_http", "key")
	if s == nil {
		t.Errorf("get err")
	}

	slog.Infoln("get test_http", s)


	s = cli.GetServAddr("test_thrift", "key1")
	if s == nil {
		t.Errorf("get err")
	}

	slog.Infoln("get test_thrift", s)

	cbfac := func(t thrift.TTransport, f thrift.TProtocolFactory) interface{} {
		return rpc.NewRpcServiceClientFactory(t, f)
	}

	ct := NewClientThrift(cli, "test_thrift", cbfac, 5)




	docall := func() {
		si, c := ct.Get("key1")
		slog.Infoln("thrift client get", si, c)

		if c == nil {
			t.Errorf("get client error")
			return
		}



		paramMap := make(map[string]string)
		paramMap["name"] = "hello"
		paramMap["passwd"] = "world"

		clicall := c.(*rpc.RpcServiceClient)
		r1, e1 := clicall.FunCall(123456, "login", paramMap)
		slog.Infoln("thrift client1 call", r1, e1)



		if e1 != nil {
			slog.Infoln("thrift client payback", r1)
			// 只有调用成功才归还
			ct.Payback(si, clicall)
		}

	}

	for i := 0; i < 50; i++ {
		go docall()
	}


	time.Sleep(time.Second * 30)

}


