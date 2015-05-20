// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package headope

import (
	"fmt"
	"time"
	"errors"

	"github.com/shawnfeng/sutil/snetutil"
	"github.com/shawnfeng/sutil/stime"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/paconn"
)

var EmptyAddrErr error = errors.New("head addrs empty")
var TimeoutErr error = errors.New("get head timeout")


func FindMaster(headAddrs []string, timeout time.Duration) (string, error) {
	fun := "HeadOpe.FindMaster"
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


type PbGetservid struct {
	//Type string
	Session string
	Servid int64

}

// 获取servid
// args
// head angent
// session 和head通信使用的session
// servid，是调用者指定的servid，head会判断合法性，不合法会返回错误，如果指定合法则ok
//        如果没有指定，则head会计算出一个可用的servid最小值返回
func GetServid(a *paconn.Agent, session string, servid int64) (int64, err) {
	res, err := a.Twoway([]byte("GET JOBS"), 200)
}
