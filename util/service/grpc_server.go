package rocserv

import (
	"context"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	Server *grpc.Server
}

type FunInterceptor func(ctx context.Context, req interface{}, fun string) error

func NewGrpcServer(fns ...FunInterceptor) *GrpcServer {

	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// add tracer interceptor
	tracer := opentracing.GlobalTracer()
	unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingServerInterceptor(tracer))
	streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamServerInterceptor(tracer))

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
		unaryInterceptors = append(unaryInterceptors, interceptor)

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
		streamInterceptors = append(streamInterceptors, streamInterceptor)
	}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return &GrpcServer{Server: server}
}
