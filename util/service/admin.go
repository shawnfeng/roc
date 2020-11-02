// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xfile"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xnet/xhttp"

	"github.com/julienschmidt/httprouter"
)

type backDoorHttp struct {
}

var (
	serviceMD5  string
	startUpTime string
)

func (m *backDoorHttp) Init() error {
	if len(os.Args) > 0 {
		filePath, err := os.Executable()
		if err == nil {
			md5, err := xfile.MD5Sum(filePath)
			if err == nil {
				serviceMD5 = fmt.Sprintf("%x", md5)
			}
		}
	}
	startUpTime = time.Now().Format("2006-01-02 15:04:05")
	return nil
}

func (m *backDoorHttp) Driver() (string, interface{}) {
	//fun := "backDoorHttp.Driver -->"

	router := httprouter.New()
	// 重启
	router.POST("/backdoor/restart", xhttp.HttpRequestWrapper(FactoryRestart))

	// healthcheck
	router.GET("/backdoor/health/check", xhttp.HttpRequestWrapper(FactoryHealthCheck))

	// 获取实例md5值
	router.GET("/backdoor/md5", xhttp.HttpRequestWrapper(FactoryMD5))

	return "0.0.0.0:60000", router
}

// ==============================
type Restart struct {
}

func FactoryRestart() xhttp.HandleRequest {
	return new(Restart)
}

func (m *Restart) Handle(r *xhttp.HttpRequest) xhttp.HttpResponse {
	xlog.Infof(context.Background(), "RECEIVE RESTART COMMAND")
	server.sbase.Stop()
	os.Exit(1)
	// 这里的代码执行不到了，因为之前已经退出了
	return xhttp.NewHttpRespString(200, "{}")
}

// ==============================
type HealthCheck struct {
}

func FactoryHealthCheck() xhttp.HandleRequest {
	return new(HealthCheck)
}

func (m *HealthCheck) Handle(r *xhttp.HttpRequest) xhttp.HttpResponse {
	fun := "HealthCheck -->"
	xlog.Infof(context.Background(), "%s in", fun)

	return xhttp.NewHttpRespString(200, "{}")
}

//MD5 ...
type MD5 struct {
}

//FactoryMD5 ...
func FactoryMD5() xhttp.HandleRequest {
	return new(MD5)
}

func (m *MD5) Handle(r *xhttp.HttpRequest) xhttp.HttpResponse {
	res := struct {
		Md5     string `json:"md5"`
		StartUp string `json:"start_up"`
	}{
		Md5:     serviceMD5,
		StartUp: startUpTime,
	}
	s, _ := json.Marshal(res)
	return xhttp.NewHttpRespString(200, string(s))
}
