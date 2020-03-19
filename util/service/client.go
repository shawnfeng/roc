// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// rpc客户端接口及部分辅助函数

package rocserv

import (
	"runtime"
	"strings"
	"time"
)

type rpcClient interface {
	Close() error
	SetTimeout(timeout time.Duration) error
	GetServiceClient() interface{}
}

// GetFunName get func name form runtime info
func GetFunName(index int) string {
	funcName := ""
	pc, _, _, ok := runtime.Caller(index)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
		if index := strings.LastIndex(funcName, "."); index != -1 {
			if len(funcName) > index+1 {
				funcName = funcName[index+1:]
			}
		}
	}
	return funcName
}