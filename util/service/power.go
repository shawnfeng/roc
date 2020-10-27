// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"
	"google.golang.org/grpc"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/gin-gonic/gin"
	"github.com/julienschmidt/httprouter"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/shawnfeng/sutil/snetutil"
	"github.com/shawnfeng/sutil/trace"
)

// rpc protocol
const (
	PROCESSOR_HTTP   = "http"
	PROCESSOR_THRIFT = "thrift"
	PROCESSOR_GRPC   = "grpc"
	PROCESSOR_GIN    = "gin"
)

const disableContextCancelKey = "disable_context_cancel"

var errNilDriver = errors.New("nil driver")

type middleware func(next http.Handler) http.Handler

type driverBuilder struct {
	c xconfig.ConfigCenter
}

func newDriverBuilder(c xconfig.ConfigCenter) *driverBuilder {
	return &driverBuilder{
		c: c,
	}
}

func (dr *driverBuilder) isDisableContextCancel(ctx context.Context) bool {
	use, ok := dr.c.GetBool(ctx, disableContextCancelKey)
	if !ok {
		return false
	}
	return use
}

func (dr *driverBuilder) powerProcessorDriver(ctx context.Context, n string, p Processor) (*ServInfo, error) {
	fun := "driverBuilder.powerProcessorDriver -> "
	addr, driver := p.Driver()
	if driver == nil {
		return nil, errNilDriver
	}

	xlog.Infof(ctx, "%s processor: %s type: %s addr: %s", fun, reflect.TypeOf(driver), addr)

	switch d := driver.(type) {
	case *httprouter.Router:
		var extraHttpMiddlewares []middleware
		if dr.isDisableContextCancel(ctx) {
			extraHttpMiddlewares = append(extraHttpMiddlewares, disableContextCancelMiddleware)
		}
		sa, err := powerHttp(addr, d, extraHttpMiddlewares...)
		if err != nil {
			return nil, err
		}
		servInfo := &ServInfo{
			Type: PROCESSOR_HTTP,
			Addr: sa,
		}
		return servInfo, nil

	case thrift.TProcessor:
		sa, err := powerThrift(addr, d)
		if err != nil {
			return nil, err
		}
		servInfo := &ServInfo{
			Type: PROCESSOR_THRIFT,
			Addr: sa,
		}
		return servInfo, nil

	case *GrpcServer:
		// 添加内部拦截器的操作必须放到NewServer中, 否则无法在服务代码中完成service注册
		sa, err := powerGrpc(addr, d)
		if err != nil {
			return nil, err
		}
		servInfo := &ServInfo{
			Type: PROCESSOR_GRPC,
			Addr: sa,
		}
		return servInfo, nil

	case *gin.Engine:
		var extraHttpMiddlewares []middleware
		if dr.isDisableContextCancel(ctx) {
			extraHttpMiddlewares = append(extraHttpMiddlewares, disableContextCancelMiddleware)
		}
		sa, err := powerGin(addr, d, extraHttpMiddlewares...)
		if err != nil {
			return nil, err
		}
		servInfo := &ServInfo{
			Type: PROCESSOR_GIN,
			Addr: sa,
		}
		return servInfo, nil

	case *HttpServer:
		var extraHttpMiddlewares []middleware
		if dr.isDisableContextCancel(ctx) {
			extraHttpMiddlewares = append(extraHttpMiddlewares, disableContextCancelMiddleware)
		}
		sa, err := powerGin(addr, d.Engine, extraHttpMiddlewares...)
		if err != nil {
			return nil, err
		}
		servInfo := &ServInfo{
			Type: PROCESSOR_GIN,
			Addr: sa,
		}
		return servInfo, nil

	default:
		return nil, fmt.Errorf("processor: %s driver not recognition", n)
	}
}

func powerHttp(addr string, router *httprouter.Router, middlewares ...middleware) (string, error) {
	fun := "powerHttp -->"
	ctx := context.Background()

	netListen, laddr, err := listenServAddr(ctx, addr)
	if err != nil {
		return "", err
	}

	// tracing
	mw := decorateHttpMiddleware(router, middlewares...)

	go func() {
		err := http.Serve(netListen, mw)
		if err != nil {
			xlog.Panicf(ctx, "%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil
}

// 打开端口监听, 并返回服务地址
func listenServAddr(ctx context.Context, addr string) (net.Listener, string, error) {
	fun := "listenServAddr --> "
	paddr, err := snetutil.GetListenAddr(addr)
	if err != nil {
		return nil, "", err
	}

	xlog.Infof(ctx, "%s config addr[%s]", fun, paddr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", paddr)
	if err != nil {
		return nil, "", err
	}

	netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		return nil, "", err
	}

	laddr, err := snetutil.GetServAddr(netListen.Addr())
	if err != nil {
		netListen.Close()
		return nil, "", err
	}

	xlog.Infof(ctx, "%s listen addr[%s]", fun, laddr)
	return netListen, laddr, nil
}

// 添加http middleware
func decorateHttpMiddleware(router http.Handler, middlewares ...middleware) http.Handler {
	r := router
	for _, m := range middlewares {
		r = m(r)
	}
	// tracing
	mw := nethttp.Middleware(
		xtrace.GlobalTracer(),
		// add logging middleware
		httpTrafficLogMiddleware(r),
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return "HTTP " + r.Method + ": " + r.URL.Path
		}),
		nethttp.MWSpanFilter(trace.UrlSpanFilter))

	return mw
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

func powerGin(addr string, router *gin.Engine, middlewares ...middleware) (string, error) {
	fun := "powerGin -->"
	ctx := context.Background()

	netListen, laddr, err := listenServAddr(ctx, addr)
	if err != nil {
		return "", err
	}

	// tracing
	mw := decorateHttpMiddleware(router, middlewares...)

	serv := &http.Server{Handler: mw}
	go func() {
		err := serv.Serve(netListen)
		if err != nil {
			xlog.Panicf(ctx, "%s laddr[%s]", fun, laddr)
		}
	}()

	return laddr, nil
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

func disableContextCancelMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newR := r.WithContext(xcontext.NewValueContext(r.Context()))
		next.ServeHTTP(w, newR)
	})
}

func newDisableContextCancelGrpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		valueCtx := xcontext.NewValueContext(ctx)
		return handler(valueCtx, req)
	}
}
