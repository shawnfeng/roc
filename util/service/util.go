package rocserv

import (
	"context"
	"runtime"
	"strings"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil"
)

const (
	// Timeout timeout(ms)
	Timeout = "timeoutMsec"
	// Default ...
	Default = "Default"
)

// deprecated
// GetFuncName get func name form runtime info
func GetFuncName(index int) string {
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

// GetFuncNameWithCtx get fun name from context, if not set then use runtime caller
func GetFuncNameWithCtx(ctx context.Context, index int) string {
	var funcName string
	if method, ok := xcontext.GetCallerMethod(ctx); ok {
		return method
	}
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

// GetFuncTimeout get func timeout conf
func GetFuncTimeout(servKey, funcName string, defaultTime time.Duration) time.Duration {
	key := xutil.Concat(servKey, ".", funcName, ".", Timeout)
	var t int
	var exist bool
	confCenter := GetConfigCenter()
	if confCenter != nil {
		if t, exist = confCenter.GetIntWithNamespace(context.TODO(), RPCConfNamespace, key); !exist {
			defaultKey := xutil.Concat(servKey, ".", Default, ".", Timeout)
			t, _ = confCenter.GetIntWithNamespace(context.TODO(), RPCConfNamespace, defaultKey)
		}
	}
	if t == 0 {
		return defaultTime
	}

	return time.Duration(t) * time.Millisecond
}
