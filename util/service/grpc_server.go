package rocserv

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	Server *grpc.Server
}

type FunInterceptor func(ctx context.Context, req interface{}, fun string) error

// NewGrpcServer create grpc server with interceptors before handler
func NewGrpcServer(fns ...FunInterceptor) *GrpcServer {

	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// add tracer、monitor interceptor
	tracer := opentracing.GlobalTracer()
	unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingServerInterceptor(tracer), monitorServerInterceptor())
	streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamServerInterceptor(tracer), monitorStreamServerInterceptor())

	// TODO 采用框架内显式注入interceptors的方式，不再进行二次包装，后续该部分功能会删除掉
	//for _, fn := range fns {
	//	// 注册interceptor
	//	var interceptor grpc.UnaryServerInterceptor
	//	interceptor = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	//		err = fn(ctx, req, info.FullMethod)
	//		if err != nil {
	//			return
	//		}
	//		// 继续处理请求
	//		return handler(ctx, req)
	//	}
	//	unaryInterceptors = append(unaryInterceptors, interceptor)
	//
	//	var streamInterceptor grpc.StreamServerInterceptor
	//	streamInterceptor = func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	//		ss.Context()
	//		err := fn(ss.Context(), srv, info.FullMethod)
	//		if err != nil {
	//			return err
	//		}
	//		// 继续处理请求
	//		return handler(srv, ss)
	//	}
	//	streamInterceptors = append(streamInterceptors, streamInterceptor)
	//}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return &GrpcServer{Server: server}
}

// server rpc cost, record to log and prometheus
func monitorServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		_metricAPIRequestCount.Inc()
		fun := info.FullMethod
		st := stime.NewTimeStat()
		resp, err = handler(ctx, req)
		dur := st.Duration()
		slog.Infof("%s req: %v ctx: %v cost: %d", fun, req, ctx, dur)
		_metricAPIRequestTime.Observe(float64(st.Millisecond()))
		return resp, err
	}
}

// stream server rpc cost, record to log and prometheus
func monitorStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		_metricAPIRequestCount.Inc()
		fun := info.FullMethod
		st := stime.NewTimeStat()
		err := handler(srv, ss)
		dur := st.Duration()
		slog.Infof("%s req: %v ctx: %v cost: %d", fun, srv, ss.Context(), dur)
		_metricAPIRequestTime.Observe(float64(st.Millisecond()))
		return err
	}
}
