package rocserv

import (
	"context"
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	Server *grpc.Server
}

type FunInterceptor func(ctx context.Context, req interface{}, fun string) error

func NewGrpcServer(fns ...FunInterceptor) *GrpcServer {
	var opts []grpc.ServerOption
	for _, fn := range fns {
		// 注册interceptor
		var interceptor grpc.UnaryServerInterceptor
		interceptor = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			err = fn(ctx, req, info.FullMethod)
			if err != nil {
				return
			}
			// 继续处理请求
			return handler(ctx, req)
		}
		opts = append(opts, grpc.UnaryInterceptor(interceptor))

		var streamInterceptor grpc.StreamServerInterceptor
		streamInterceptor = func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ss.Context()
			err := fn(ss.Context(), srv, info.FullMethod)
			if err != nil {
				return err
			}
			// 继续处理请求
			return handler(srv, ss)
		}
		opts = append(opts, grpc.StreamInterceptor(streamInterceptor))
	}

	// tracing
	tracer := opentracing.GlobalTracer()
	opts = append(opts, grpc.UnaryInterceptor(
		otgrpc.OpenTracingServerInterceptor(tracer)))
	opts = append(opts, grpc.StreamInterceptor(
		otgrpc.OpenTracingStreamServerInterceptor(tracer)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return &GrpcServer{Server: server}
}
