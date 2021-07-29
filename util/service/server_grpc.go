package rocserv

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xtransport/gen-go/util/thriftutil"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/opentracing/opentracing-go"
	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	otgrpc "gitlab.pri.ibanyu.com/tracing/go-grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const logRequestKey = "log_request"

type printBodyMethod struct {
	LogRequestMethodList   []string `json:"log_request_method_list" properties:"log_request_method_list"`
	NoLogRequestMethodList []string `json:"no_log_request_method_list" properties:"no_log_request_method_list"`
}

type GrpcServer struct {
	userUnaryInterceptors  []grpc.UnaryServerInterceptor
	extraUnaryInterceptors []grpc.UnaryServerInterceptor // 服务启动之前, 内部添加的拦截器, 在所有拦截器之后添加
	Server                 *grpc.Server
}

type FunInterceptor func(ctx context.Context, req interface{}, fun string) error

// UnaryHandler是grpc UnaryHandler的别名, 便于统一管理grpc升级
type UnaryHandler func(ctx context.Context, req interface{}) (interface{}, error)

// UnaryServerInterceptor是grpc UnaryServerInterceptor的别名, 便于统一管理grpc升级
type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (interface{}, error)

// UnaryServerInfo是grpc UnaryServerInfo的别名, 便于统一管理grpc升级
type UnaryServerInfo struct {
	// Server is the service implementation the user provides. This is read-only.
	Server interface{}
	// FullMethod is the full RPC method string, i.e., /package.service/method.
	FullMethod string
}

// Deprecated
// NewGrpcServer create grpc server with interceptors before handler
func NewGrpcServer(fns ...FunInterceptor) *GrpcServer {
	return NewGrpcServerWithUnaryInterceptors()
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
	userUnaryInterceptors := convertUnaryInterceptors(interceptors...)

	gserv := &GrpcServer{
		userUnaryInterceptors: userUnaryInterceptors,
	}

	// gRPC注册服务是在服务模板代码中的, 所以只能在NewServer时添加拦截器, 才能保证模板代码不需要调整
	gserv.addExtraContextCancelInterceptor()

	s, err := gserv.buildServer()
	if err != nil {
		panic(err)
	}
	gserv.Server = s
	return gserv
}

func (g *GrpcServer) addExtraContextCancelInterceptor() {
	f := "GrpcServer.addExtraContextCancelInterceptor --> "
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	dr := newDriverBuilder(GetConfigCenter())
	disableContextCancel := dr.isDisableContextCancel(ctx)
	xlog.Infof(ctx, "%s disableContextCancel: %v", f, disableContextCancel)
	if disableContextCancel {
		contextCancelInterceptor := newDisableContextCancelGrpcUnaryInterceptor()
		g.internalAddExtraInterceptors(contextCancelInterceptor)
	}
}

func (g *GrpcServer) internalAddExtraInterceptors(extraInterceptors ...grpc.UnaryServerInterceptor) {
	g.extraUnaryInterceptors = append(g.extraUnaryInterceptors, extraInterceptors...)
}

func (g *GrpcServer) buildServer() (*grpc.Server, error) {
	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// add tracer、monitor、recovery interceptor
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(recoveryFunc),
	}
	unaryInterceptors = append(unaryInterceptors,
		grpc_recovery.UnaryServerInterceptor(recoveryOpts...),
		otgrpc.OpenTracingServerInterceptorWithGlobalTracer(otgrpc.SpanDecorator(apmSetSpanTagDecorator)),
		rateLimitInterceptor(),
		monitorServerInterceptor(),
		laneInfoServerInterceptor(),
		headInfoServerInterceptor(),
	)
	userUnaryInterceptors := g.userUnaryInterceptors
	unaryInterceptors = append(unaryInterceptors, userUnaryInterceptors...)
	unaryInterceptors = append(unaryInterceptors, g.extraUnaryInterceptors...)

	streamInterceptors = append(streamInterceptors, rateLimitStreamServerInterceptor(), otgrpc.OpenTracingStreamServerInterceptorWithGlobalTracer(), monitorStreamServerInterceptor(), grpc_recovery.StreamServerInterceptor(recoveryOpts...))

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)))

	// 实例化grpc Server
	server := grpc.NewServer(opts...)
	return server, nil
}

func apmSetSpanTagDecorator(ctx context.Context, span opentracing.Span, method string, req, resp interface{}, grpcError error) {
	var hasError bool
	if ctx.Err() != nil {
		span.SetTag("error.ctx", true)
		hasError = true
	}
	if grpcError != nil {
		span.SetTag("error.grpc", true)
		hasError = true
	}
	if hasError {
		span.SetTag("error", true)
	}
	// set instance info tags
	sb := GetServBase()
	if sb != nil {
		span.SetTag("region", sb.Region())
		span.SetTag("ip", sb.ServIp())
		span.SetTag("lane", sb.Lane())
	}
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
		caller := GetCallerFromBaggage(ctx)
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
		//TODO 先做兼容，后续再补上
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun, xprom.LabelErrCode, "1").Inc()
		st := xtime.NewTimeStat()
		resp, err = handler(ctx, req)
		if shouldLogRequest(info.FullMethod) {
			xlog.Infow(ctx, "", "func", fun, "req", req, "err", err, "cost", st.Millisecond(), "resp", resp)
		} else {
			xlog.Infow(ctx, "", "func", fun, "err", err, "cost", st.Millisecond())
		}
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun, xprom.LabelErrCode, "1").Observe(float64(st.Millisecond()))
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
		//TODO 先做兼容，后续再补上
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun, xprom.LabelErrCode, "1").Inc()
		st := xtime.NewTimeStat()
		err := handler(srv, ss)
		if shouldLogRequest(info.FullMethod) {
			xlog.Infow(ss.Context(), "", "func", fun, "req", srv, "err", err, "cost", st.Millisecond())
		} else {
			xlog.Infow(ss.Context(), "", "func", fun, "err", err, "cost", st.Millisecond())
		}
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, service, xprom.LabelAPI, fun, xprom.LabelErrCode, "1").Observe(float64(st.Millisecond()))
		return err
	}
}

func laneInfoServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}
		var lane string
		lanes := md[LaneInfoMetadataKey]
		if len(lanes) >= 1 {
			lane = lanes[0]
		}

		route := thriftutil.NewRoute()
		route.Group = lane
		control := thriftutil.NewControl()
		control.Route = route

		ctx = context.WithValue(ctx, xcontext.ContextKeyControl, control)
		return handler(ctx, req)
	}
}

func headInfoServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}
		var hlc string
		values := md[xcontext.ContextPropertiesKeyHLC]
		if len(values) >= 1 {
			hlc = values[0]
		}
		ctx = xcontext.SetHeaderPropertiesHLC(ctx, hlc)
		return handler(ctx, req)
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

func shouldLogRequest(fullMethod string) bool {
	// 默认打印
	methodName, err := getMethodName(fullMethod)
	if err != nil {
		return true
	}
	center := GetConfigCenter()
	if center == nil {
		return true
	}
	printBodyMethod := printBodyMethod{}

	// 方法配置
	_ = center.Unmarshal(context.Background(), &printBodyMethod)
	// 不打印的优先级更高
	if methodInList(methodName, printBodyMethod.NoLogRequestMethodList) {
		return false
	}
	if methodInList(methodName, printBodyMethod.LogRequestMethodList) {
		return true
	}

	// 全局配置
	isPrint, ok := center.GetBool(context.Background(), logRequestKey)
	if !ok {
		// 默认输出
		return true
	}
	return isPrint
}

// FullMethod is the full RPC method string, i.e., /package.service/method
func getMethodName(fullMethod string) (string, error) {
	arr := strings.Split(fullMethod, "/")
	if len(arr) < 3 {
		return "", errors.New("full method is invalid")
	}
	// 根据格式/package.service/method，切割后，取method
	return arr[2], nil
}

// 方法是否在列表中
func methodInList(name string, list []string) bool {
	for _, l := range list {
		if name == l {
			return true
		}
	}
	return false
}
