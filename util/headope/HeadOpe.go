package headope

import (
	"fmt"
	"time"
	"errors"
	"github.com/shawnfeng/sutil/snetutil"
	"github.com/shawnfeng/sutil/stime"
	"github.com/shawnfeng/sutil/slog"
)

var EmptyAddrErr error = errors.New("head addrs empty")
var TimeoutErr error = errors.New("get head timeout")


func findMaster(headAddrs []string, timeout time.Duration) (string, error) {
	fun := "HeadOpe.findMaster"
	if len(headAddrs) == 0 {
		return "", EmptyAddrErr
	}

	backOff := stime.NewBackOffCtrl(
		time.Millisecond * 100,
		time.Second * 1,
	)

	st := stime.NewTimeStat()
	for {
		for _, h := range headAddrs {
			url := fmt.Sprintf("http://%s/get/master", h)
			body, err := snetutil.HttpReqGetOk(url, time.Millisecond * 200)
			if err != nil {
				slog.Warnf("%s get head:%s err:%s", fun, url, err)
			} else {
				return string(body), nil
			}

		}

		if st.Duration() >= timeout {
			return "", TimeoutErr
		}

		backOff.BackOff()
	}
}
