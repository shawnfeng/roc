package rocserv

import (
	"context"
	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit"
	"runtime"
	"strings"

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
