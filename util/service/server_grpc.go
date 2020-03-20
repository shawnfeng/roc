package rocserv

import (
	"context"

	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/slog/slog"
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
		group, service := GetGroupAndService()
		fun := info.FullMethod
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Inc()
		st := stime.NewTimeStat()
		resp, err = handler(ctx, req)
		slog.Infof(ctx, "%s req: %v err: %v cost: %d us", fun, req, err, st.Microsecond())
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Observe(float64(st.Millisecond()))
		return resp, err
	}
}

// stream server rpc cost, record to log and prometheus
func monitorStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fun := info.FullMethod
		group, service := GetGroupAndService()
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Inc()
		st := stime.NewTimeStat()
		err := handler(srv, ss)
		slog.Infof(ss.Context(), "%s req: %v err: %v cost: %d us", fun, srv, err, st.Microsecond())
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun).Observe(float64(st.Millisecond()))
		return err
	}
}
