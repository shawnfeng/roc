package rocserv

import (
	"context"
	"runtime"
	"strings"

	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/opentracing-contrib/go-grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GrpcServer struct {
	Server *grpc.Server
}

type FunInterceptor func(ctx context.Context, req interface{}, fun string) error

// UnaryHandler是grpc UnaryHandler的别名, 便于统一管理grpc升级
type UnaryHandler func(ctx context.Context, req interface{}) (interface{}, error)

// UnaryServerInterceptor是grpc UnaryServerInterceptor的别名, 便于统一管理grpc升级
type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (interface{}, error)

// UnaryServerInfo是grpc UnaryServerInfo的别名,
type UnaryServerInfo struct {
	// Server is the service implementation the user provides. This is read-only.
	Server interface{}
	// FullMethod is the full RPC method string, i.e., /package.service/method.
	FullMethod string
}

// NewGrpcServer create grpc server with interceptors before handler
func NewGrpcServer(fns ...FunInterceptor) *GrpcServer {

	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// add tracer、monitor、recovery interceptor
	tracer := xtrace.GlobalTracer()
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(recoveryFunc),
	}
	unaryInterceptors = append(unaryInterceptors, rateLimitInterceptor(), otgrpc.OpenTracingServerInterceptor(tracer), monitorServerInterceptor(), grpc_recovery.UnaryServerInterceptor(recoveryOpts...))
	streamInterceptors = append(streamInterceptors, rateLimitStreamServerInterceptor(), otgrpc.OpenTracingStreamServerInterceptor(tracer), monitorStreamServerInterceptor(), grpc_recovery.StreamServerInterceptor(recoveryOpts...))

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return &GrpcServer{Server: server}
}

// NewGrpcServerWithUnaryInterceptors 创建GrpcServer并添加自定义Unary拦截器
// 拦截器的调用顺序与添加顺序前向相同, 后向相反. 即如果添加顺序为: a, b.
// 则调用链顺序为: pre-a -> pre-b -> grpc_func() -> post-b -> post-a
// 这里添加的是用户自定义拦截器, 会添加在系统内置拦截器之后 (如tracing, 熔断等).
// 示例 (对应于你的service项目中的processor/proc_grpc.go文件)
//func (m *ProcGrpc) Driver() (string, interface{}) {
//	monitorInterceptor := func(ctx context.Context, req interface{}, info *rocserv.UnaryServerInfo, handler rocserv.UnaryHandler) (interface{}, error) {
//
//		// 这里添加接口调用前的拦截器处理逻辑
//		// e.g. 统计接口请求耗时
//		st := xtime.NewTimeStat()
//		defer func() {
//			dur := st.Duration()
//			xlog.Infof(ctx, "monitor example, func: %s, req: %v, ctx: %v, duration: %v", info.FullMethod, req, ctx, dur)
//		}()
//
//		// gRPC接口调用 (固定写法)
//		ret, err := handler(ctx, req)
//
//		// 这里添加接口调用后的拦截器处理逻辑
//		// e.g. 接口调用出错时打error日志
//		if err != nil {
//			xlog.Warnf(ctx, "call grpc error, func: %s, req: %v, err: %v", info.FullMethod, req, err)
//		}
//
//		return ret, err
//	}
//	serv := rocserv.NewGrpcServerWithInterceptors(monitorInterceptor)
//	RegisterChangeBoardServiceServer(serv.Server, new(GrpcChangeBoardServiceImpl))
//	// 使用随机端口
//	return "", serv
//}
func NewGrpcServerWithUnaryInterceptors(interceptors ...UnaryServerInterceptor) *GrpcServer {
	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// add tracer、monitor、recovery interceptor
	tracer := xtrace.GlobalTracer()
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(recoveryFunc),
	}
	unaryInterceptors = append(unaryInterceptors, rateLimitInterceptor(), otgrpc.OpenTracingServerInterceptor(tracer), monitorServerInterceptor(), grpc_recovery.UnaryServerInterceptor(recoveryOpts...))
	userUnaryInterceptors := convertUnaryInterceptors(interceptors...)
	unaryInterceptors = append(unaryInterceptors, userUnaryInterceptors...)

	streamInterceptors = append(streamInterceptors, rateLimitStreamServerInterceptor(), otgrpc.OpenTracingStreamServerInterceptor(tracer), monitorStreamServerInterceptor(), grpc_recovery.StreamServerInterceptor(recoveryOpts...))

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return &GrpcServer{Server: server}
}

func convertUnaryInterceptors(interceptors ...UnaryServerInterceptor) []grpc.UnaryServerInterceptor {
	var ret []grpc.UnaryServerInterceptor
	for _, interceptor := range interceptors {
		ret = append(ret, convertUnaryInterceptor(interceptor))
	}
	return ret
}

func convertUnaryInterceptor(interceptor UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		return interceptor(ctx, req, convertUnaryServerInfo(info), convertUnaryHandler(handler))
	}
}

func convertUnaryHandler(handler grpc.UnaryHandler) UnaryHandler {
	return UnaryHandler(handler)
}

func convertUnaryServerInfo(info *grpc.UnaryServerInfo) *UnaryServerInfo {
	return &UnaryServerInfo{
		Server:     info.Server,
		FullMethod: info.FullMethod,
	}
}

// rate limiter interceptor, should be before OpenTracingServerInterceptor and monitorServerInterceptor
func rateLimitInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		parts := strings.Split(info.FullMethod, "/")
		interfaceName := parts[len(parts)-1]

		// 暂时不支持按照调用方限流
		caller := UNSPECIFIED_CALLER
		err = rateLimitRegistry.InterfaceRateLimit(ctx, interfaceName, caller)
		if err != nil {
			if err == rate_limit.ErrRateLimited {
				xlog.Warnf(ctx, "rate limited: method=%s, caller=%s", info.FullMethod, caller)
			}
			return nil, err
		} else {
			return handler(ctx, req)
		}
	}
}

// server rpc cost, record to log and prometheus
func monitorServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		group, service := GetGroupAndService()
		fun := info.FullMethod
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Inc()
		st := xtime.NewTimeStat()
		resp, err = handler(ctx, req)
		xlog.Infow(ctx, "", "func", fun, "req", req, "err", err, "cost", st.Millisecond())
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Observe(float64(st.Millisecond()))
		return resp, err
	}
}

// rate limiter interceptor, should be before OpenTracingStreamServerInterceptor and monitorStreamServerInterceptor
func rateLimitStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := context.Background()
		parts := strings.Split(info.FullMethod, "/")
		interfaceName := parts[len(parts)-1]

		// 暂时不支持按照调用方限流
		caller := UNSPECIFIED_CALLER
		err := rateLimitRegistry.InterfaceRateLimit(ctx, interfaceName, caller)
		if err != nil {
			if err == rate_limit.ErrRateLimited {
				xlog.Warnf(ctx, "rate limited: method=%s, caller=%s", info.FullMethod, caller)
			}
			return err
		} else {
			return handler(ctx, ss)
		}
	}
}

// stream server rpc cost, record to log and prometheus
func monitorStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fun := info.FullMethod
		group, service := GetGroupAndService()
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Inc()
		st := xtime.NewTimeStat()
		err := handler(srv, ss)
		xlog.Infow(ss.Context(), "", "func", fun, "req", srv, "err", err, "cost", st.Millisecond())
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Observe(float64(st.Millisecond()))
		return err
	}
}

func recoveryFunc(p interface{}) (err error) {
	ctx := context.Background()
	const size = 4096
	buf := make([]byte, size)
	buf = buf[:runtime.Stack(buf, false)]
	xlog.Errorf(ctx, "%v catch panic, stack: %s", p, string(buf))
	return status.Errorf(codes.Internal, "panic triggered: %v", p)
}
