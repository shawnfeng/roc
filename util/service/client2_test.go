// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"testing"
	"time"
	"fmt"

	"github.com/shawnfeng/sutil/slog"

)

func TestClient(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}

	cli, err := NewClientLookup(etcds, "roc", "base/account")

	slog.Infof("Test client:%s err:%v", cli, err)

	if err != nil {
		t.Errorf("create err:%s", err)
		return
	}
	time.Sleep(time.Second * 2)

	//allserv := cli.GetAllServAddr()
	//slog.Infoln("ALL", allserv)


	s := cli.GetServAddr("noexit", "key")
	if s != nil {
		t.Errorf("get err")
	}

	var keys []string
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("%d", i))
	}

	count := make(map[string]int)

	for _, k := range keys {
		s = cli.GetServAddr("proc_thrift", k)
		if s == nil {
			t.Errorf("get err")
		}
		//slog.Infoln("get test_thrift", s)
		ss := s.String()
		if _, ok := count[ss]; !ok {
			count[ss] = 0
		}
		count[ss] += 1
	}

	for k, v := range count {
		slog.Infoln("stat", k, v)
	}


	s = cli.GetServAddrWithServid(3, "proc_thrift", "key")
	if s == nil {
		t.Errorf("get err")
	}

	slog.Infoln("get test_thrift", s)


}


