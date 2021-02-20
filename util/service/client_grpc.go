package rocserv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"
	otgrpc "gitlab.pri.ibanyu.com/tracing/go-grpc"

	"github.com/uber/jaeger-client-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ServProtocol int

const (
	GRPC ServProtocol = iota
	THRIFT
	HTTP

	LaneInfoMetadataKey = "ipalfish-lane-info"
)

// ClientGrpc client of grpc in adapter
type ClientGrpc struct {
	fallbacks

	clientLookup ClientLookup
	processor    string
	breaker      *Breaker
	router       Router

	pool      *ClientPool
	fnFactory func(conn *grpc.ClientConn) interface{}
}

type Provider struct {
	Ip   string
	Port uint16
}

// NewClientGrpcWithRouterType create grpc client by routerType, fn: xxServiceClient of xx, such as NewChangeBoardServiceClient
func NewClientGrpcWithRouterType(cb ClientLookup, processor string, capacity int, fn func(client *grpc.ClientConn) interface{}, routerType int) *ClientGrpc {
	clientGrpc := &ClientGrpc{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
		fnFactory:    fn,
	}
	// 目前为写死值，后期改为动态配置获取的方式
	pool := NewClientPool(defaultMaxIdle, defaultMaxActive, clientGrpc.newConn, cb.ServKey())
	clientGrpc.pool = pool
	cb.RegisterDeleteAddrHandler(clientGrpc.deleteAddrHandler)
	return clientGrpc
}

func (m *ClientGrpc) deleteAddrHandler(addrs []string) {
	for _, addr := range addrs {
		deleteAddrFromConnPool(addr, m.pool)
	}
}

func NewClientGrpcByConcurrentRouter(cb ClientLookup, processor string, capacity int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, capacity, fn, 1)
}

func NewClientGrpc(cb ClientLookup, processor string, capacity int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, capacity, fn, 0)
}

func (m *ClientGrpc) CustomizedRouteRpc(getProvider func() *Provider, fnrpc func(interface{}) error) error {
	if getProvider == nil {
		return errors.New("fun getProvider is nil")
	}
	provider := getProvider()
	return m.DirectRouteRpc(provider, fnrpc)
}

func (m *ClientGrpc) DirectRouteRpc(provider *Provider, fnrpc func(interface{}) error) error {
	if provider == nil {
		return errors.New("get Provider is nil")
	}
	si, rc, e := m.getClient(provider)
	if e != nil {
		return e
	}
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}
	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(_ctx context.Context) error {
		return m.rpc(si, rc, fnrpc)
	}

	funcName := GetFuncName(3)
	var err error
	st := xtime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(context.Background(), funcName, call, m.GetFallbackFunc(funcName))
	return err
}

func (m *ClientGrpc) getClient(provider *Provider) (*ServInfo, rpcClientConn, error) {
	servInfos := m.clientLookup.GetAllServAddr(m.processor)
	if len(servInfos) < 1 {
		return nil, nil, errors.New(m.processor + " server provider is emtpy ")
	}
	var serv *ServInfo
	addr := fmt.Sprintf("%s:%d", provider.Ip, provider.Port)
	for _, item := range servInfos {
		if item.Addr == addr {
			serv = item
			break
		}
	}
	if serv == nil {
		return nil, nil, errors.New(m.processor + " server provider is emtpy ")
	}
	conn, err := m.pool.Get(context.Background(), serv.Addr)
	return serv, conn, err
}

// deprecated
func (m *ClientGrpc) Rpc(hashKey string, fnrpc func(interface{}) error) error {
	return m.RpcWithContext(context.TODO(), hashKey, fnrpc)
}

func (m *ClientGrpc) RpcWithContext(ctx context.Context, hashKey string, fnrpc func(interface{}) error) error {
	var err error
	funcName := GetFuncName(3)
	if funcName == "grpcInvoke" {
		funcName = GetFuncName(4)
	}
	retry := GetFuncRetry(m.clientLookup.ServKey(), funcName)
	for ; retry >= 0; retry-- {
		err = m.do(ctx, hashKey, funcName, fnrpc)
		if err == nil {
			return nil
		}
	}
	return err
}

func (m *ClientGrpc) do(ctx context.Context, hashKey, funcName string, fnrpc func(interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find grpc service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(_ctx context.Context) error {
		return m.rpc(si, rc, fnrpc)
	}

	var err error
	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(ctx, funcName, call, m.GetFallbackFunc(funcName))
	return err
}

func (m *ClientGrpc) RpcWithContextV2(ctx context.Context, hashKey string, fnrpc func(context.Context, interface{}) error) error {
	var err error
	funcName := GetFuncNameWithCtx(ctx, 3)
	retry := GetFuncRetry(m.clientLookup.ServKey(), funcName)
	for ; retry >= 0; retry-- {
		err = m.doWithContext(ctx, hashKey, funcName, fnrpc)
		if err == nil {
			return nil
		}
	}
	return err
}

func (m *ClientGrpc) doWithContext(ctx context.Context, hashKey, funcName string, fnrpc func(context.Context, interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find grpc service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	ctx = m.injectServInfo(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(ctx context.Context) error {
		return m.rpcWithContext(ctx, si, rc, fnrpc)
	}

	var err error
	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(ctx, funcName, call, m.GetFallbackFunc(funcName))
	return err
}

func (m *ClientGrpc) rpc(si *ServInfo, rc rpcClientConn, fnrpc func(interface{}) error) error {
	c := rc.GetServiceClient()
	err := fnrpc(c)
	m.pool.Put(si.Addr, rc, err)
	return err
}

func (m *ClientGrpc) rpcWithContext(ctx context.Context, si *ServInfo, rc rpcClientConn, fnrpc func(context.Context, interface{}) error) error {
	c := rc.GetServiceClient()
	err := fnrpc(ctx, c)
	m.pool.Put(si.Addr, rc, err)
	return err
}

func (m *ClientGrpc) route(ctx context.Context, key string) (*ServInfo, rpcClientConn) {
	s := m.router.Route(ctx, m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	conn, _ := m.pool.Get(ctx, addr)
	return s, conn
}

func (m *ClientGrpc) injectServInfo(ctx context.Context, si *ServInfo) context.Context {
	ctx, err := xcontext.SetControlCallerServerName(ctx, serviceFromServPath(m.clientLookup.ServPath()))
	if err != nil {
		return ctx
	}

	ctx, err = xcontext.SetControlCallerServerID(ctx, fmt.Sprint(si.Servid))
	if err != nil {
		return ctx
	}

	span := xtrace.SpanFromContext(ctx)
	if span == nil {
		return ctx
	}

	if jaegerSpan, ok := span.(*jaeger.Span); ok {
		ctx, err = xcontext.SetControlCallerMethod(ctx, jaegerSpan.OperationName())
	}
	return ctx
}

//func (m *ClientGrpc) logTraffic(ctx context.Context, si *ServInfo) {
//	kv := make(map[string]interface{})
//	for k, v := range trafficKVFromContext(ctx) {
//		kv[k] = v
//	}
//
//	kv[TrafficLogKeyServerType] = si.Type
//	kv[TrafficLogKeyServerID] = si.Servid
//	kv[TrafficLogKeyServerName] = serviceFromServPath(m.clientLookup.ServPath())
//	logTrafficByKV(ctx, kv)
//}

type grpcClientConn struct {
	serviceClient interface{}
	conn          *grpc.ClientConn
}

func (m *grpcClientConn) SetTimeout(timeout time.Duration) error {
	return fmt.Errorf("SetTimeout is not support ")
}

func (m *grpcClientConn) Close() error {
	if m.conn != nil {
		m.conn.Close()
	}
	return nil
}

func (m *grpcClientConn) GetServiceClient() interface{} {
	return m.serviceClient
}

// factory function in client connection pool
func (m *ClientGrpc) newConn(addr string) (rpcClientConn, error) {
	fun := "ClientGrpc.newConn-->"

	// 可加入多种拦截器
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			otgrpc.OpenTracingClientInterceptorWithGlobalTracer(otgrpc.SpanDecorator(apmSetSpanTagDecorator))),
		grpc.WithUnaryInterceptor(
			LaneInfoUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(
			otgrpc.OpenTracingStreamClientInterceptorWithGlobalTracer()),
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		xlog.Errorf(context.Background(), "%s dial addr: %s failed, err: %v", fun, addr, err)
		return nil, err
	}
	client := m.fnFactory(conn)
	return &grpcClientConn{
		serviceClient: client,
		conn:          conn,
	}, nil
}

func LaneInfoUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		lane, _ := xcontext.GetControlRouteGroup(ctx)
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		md.Set(LaneInfoMetadataKey, lane)
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	}
}
