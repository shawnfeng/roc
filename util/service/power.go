// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace/spanfilter"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/gin-gonic/gin"
	"github.com/julienschmidt/httprouter"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/shawnfeng/sutil/snetutil"
)

func powerHttp(addr string, router *httprouter.Router) (string, error) {
	fun := "powerHttp -->"
	ctx := context.Background()

	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}

	xlog.Infof(ctx, "%s config addr[%s]", fun, paddr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", paddr)
	if err != nil {
		return "", err
	}

	netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		return "", err
	}

	laddr, err := snetutil.GetServAddr(netListen.Addr())
	if err != nil {
		netListen.Close()
		return "", err
	}

	xlog.Infof(ctx, "%s listen addr[%s]", fun, laddr)

	// tracing
	mw := nethttp.Middleware(
		xtrace.GlobalTracer(),
		// add logging middleware
		httpTrafficLogMiddleware(router),
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return "HTTP " + r.Method + ": " + r.URL.Path
		}),
		nethttp.MWSpanFilter(spanfilter.UrlSpanFilter))

	go func() {
		err := http.Serve(netListen, mw)
		if err != nil {
			xlog.Panicf(ctx, "%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil
}

func powerThrift(addr string, processor thrift.TProcessor) (string, error) {
	fun := "powerThrift -->"
	ctx := context.Background()

	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}

	xlog.Infof(ctx, "%s config addr[%s]", fun, paddr)

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	//protocolFactory := thrift.NewTCompactProtocolFactory()

	serverTransport, err := thrift.NewTServerSocket(paddr)
	if err != nil {
		return "", err
	}

	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	// Listen后就可以拿到端口了
	//err = server.Listen()
	err = serverTransport.Listen()
	if err != nil {
		return "", err
	}

	laddr, err := snetutil.GetServAddr(serverTransport.Addr())
	if err != nil {
		return "", err
	}

	xlog.Infof(ctx, "%s listen addr[%s]", fun, laddr)

	go func() {
		err := server.Serve()
		if err != nil {
			xlog.Panicf(ctx, "%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil

}

//启动grpc ，并返回端口信息
func powerGrpc(addr string, server *GrpcServer) (string, error) {
	fun := "powerGrpc -->"
	ctx := context.Background()
	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", err
	}
	xlog.Infof(ctx, "%s config addr[%s]", fun, paddr)
	lis, err := net.Listen("tcp", paddr)
	if err != nil {
		return "", fmt.Errorf("grpc tcp Listen err:%v", err)
	}
	laddr, err := snetutil.GetServAddr(lis.Addr())
	if err != nil {
		return "", fmt.Errorf(" GetServAddr err:%v", err)
	}
	xlog.Infof(ctx, "%s listen grpc addr[%s]", fun, laddr)
	go func() {
		if err := server.Server.Serve(lis); err != nil {
			xlog.Panicf(ctx, "%s grpc laddr[%s]", fun, laddr)
		}
	}()
	return laddr, nil
}

func powerGin(addr string, router *gin.Engine) (string, *http.Server, error) {
	fun := "powerGin -->"
	ctx := context.Background()

	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return "", nil, err
	}

	xlog.Infof(ctx, "%s config addr[%s]", fun, paddr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", paddr)
	if err != nil {
		return "", nil, err
	}

	netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		return "", nil, err
	}

	laddr, err := snetutil.GetServAddr(netListen.Addr())
	if err != nil {
		netListen.Close()
		return "", nil, err
	}

	xlog.Infof(ctx, "%s listen addr[%s]", fun, laddr)

	// tracing
	mw := nethttp.Middleware(
		xtrace.GlobalTracer(),
		httpTrafficLogMiddleware(router),
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return "HTTP " + r.Method + ": " + r.URL.Path
		}),
		nethttp.MWSpanFilter(spanfilter.UrlSpanFilter))

	serv := &http.Server{Handler: mw}
	go func() {
		err := serv.Serve(netListen)
		if err != nil {
			xlog.Panicf(ctx, "%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, serv, nil
}

func reloadRouter(processor string, server interface{}, driver interface{}) error {
	fun := "reloadRouter -->"

	s, ok := server.(*http.Server)
	if !ok {
		return fmt.Errorf("server type error")
	}

	switch router := driver.(type) {
	case *gin.Engine:
		mw := nethttp.Middleware(
			xtrace.GlobalTracer(),
			router,
			nethttp.OperationNameFunc(func(r *http.Request) string {
				return "HTTP " + r.Method + ": " + r.URL.Path
			}))
		s.Handler = mw
		xlog.Infof(context.Background(), "%s reload ok, processors:%s", fun, processor)
	default:
		return fmt.Errorf("processor:%s driver not recognition", processor)
	}

	return nil
}
