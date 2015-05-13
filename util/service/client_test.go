package rocserv

import (
	"testing"
	"time"
	"github.com/shawnfeng/sutil/slog"
)

func TestClient(t *testing.T) {
    etcds := []string{"http://127.0.0.1:20002"}

	cli, err := NewClientEtcd(etcds, "/disp/service/niubi", "fuck")

	slog.Infof("Test client:%s err:%v", cli, err)

	if err != nil {
		t.Errorf("create err:%s", err)
		return
	}
	time.Sleep(time.Second * 2)


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


	time.Sleep(time.Second * 30)

}


