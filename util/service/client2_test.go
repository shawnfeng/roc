// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"testing"
	"time"

	"github.com/shawnfeng/sutil/slog"

)

func TestClient(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}

	cli, err := NewClientLookup(etcds, "roc", "push/push")

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

	s = cli.GetServAddr("proc_thrift", "key")
	if s == nil {
		t.Errorf("get err")
	}

	slog.Infoln("get test_thrift", s)


	s = cli.GetServAddrWithServid(4, "proc_thrift", "key")
	if s == nil {
		t.Errorf("get err")
	}

	slog.Infoln("get test_thrift", s)


}


