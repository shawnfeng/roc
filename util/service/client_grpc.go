package rocserv

import (
	"context"
	"errors"
	"fmt"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/scontext"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"github.com/uber/jaeger-client-go"
	"google.golang.org/grpc"
)

type ServProtocol int

const (
	GRPC ServProtocol = iota
	THRIFT
	HTTP
)

// ClientGrpc client of grpc in adapter
type ClientGrpc struct {
	clientLookup ClientLookup
	processor    string
	breaker      *Breaker
	router       Router

	pool         *ClientPool
	fnFactory    func(conn *grpc.ClientConn) interface{}
}

type Provider struct {
	Ip   string
	Port uint16
}

// NewClientGrpcWithRouterType create grpc client by routerType, fn: xxServiceClient of xx, such as NewChangeBoardServiceClient
func NewClientGrpcWithRouterType(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}, routerType int) *ClientGrpc {
	clientGrpc := &ClientGrpc{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
		fnFactory:    fn,
	}
	pool := NewClientPool(poollen, clientGrpc.newConn)
	clientGrpc.pool = pool

	return clientGrpc
}

func NewClientGrpcByConcurrentRouter(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, poollen, fn, 1)
}

func NewClientGrpc(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, poollen, fn, 0)
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

	call := func(si *ServInfo, rc rpcClientConn, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, fnrpc)
		}
	}(si, rc, fnrpc)
	funcName := GetFunName(3)
	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, GRPC, nil)
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
	conn, err := m.pool.Get(serv.Addr)
	return serv, conn, err
}

// deprecated
func (m *ClientGrpc) Rpc(hashKey string, fnrpc func(interface{}) error) error {
	return m.RpcWithContext(context.TODO(), hashKey, fnrpc)
}

func (m *ClientGrpc) RpcWithContext(ctx context.Context, hashKey string, fnrpc func(interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find grpc service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClientConn, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, fnrpc)
		}
	}(si, rc, fnrpc)

	// 目前Adapter内通过Rpc函数调用RpcWithContext时层次会出错，直接调用RpcWithContext和RpcWithContextV2的层次是正确的，所以修正前者进行兼容
	funcName := GetFunName(3)
	if funcName == "rpc" {
		funcName = GetFunName(4)
	}

	var err error
	st := stime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, GRPC, nil)
	return err
}

func (m *ClientGrpc) RpcWithContextV2(ctx context.Context, hashKey string, fnrpc func(context.Context, interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find grpc service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.logTraffic(ctx, si)
	ctx = m.injectServInfo(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClientConn, fnrpc func(context.Context, interface{}) error) func() error {
		return func() error {
			return m.rpcWithContext(ctx, si, rc, fnrpc)
		}
	}(si, rc, fnrpc)

	funcName := GetFunName(3)

	var err error
	st := stime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, GRPC, nil)
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
	conn,_:=m.pool.Get(addr)
	return s,conn
}

func (m *ClientGrpc) injectServInfo(ctx context.Context, si *ServInfo) context.Context {
	ctx, err := scontext.SetControlCallerServerName(ctx, serviceFromServPath(m.clientLookup.ServPath()))
	if err != nil {
		return ctx
	}

	ctx, err = scontext.SetControlCallerServerId(ctx, fmt.Sprint(si.Servid))
	if err != nil {
		return ctx
	}

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ctx
	}

	if jaegerSpan, ok := span.(*jaeger.Span); ok {
		ctx, err = scontext.SetControlCallerMethod(ctx, jaegerSpan.OperationName())
	}
	return ctx
}

func (m *ClientGrpc) logTraffic(ctx context.Context, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv[TrafficLogKeyServerType] = si.Type
	kv[TrafficLogKeyServerID] = si.Servid
	kv[TrafficLogKeyServerName] = serviceFromServPath(m.clientLookup.ServPath())
	logTrafficByKV(ctx, kv)
}

type grpcClientConn struct {
	serviceClient interface{}
	conn          *grpc.ClientConn
}

func (m *grpcClientConn) SetTimeout(timeout time.Duration) error {
	return fmt.Errorf("SetTimeout is not support ")
}

func (m *grpcClientConn) Close() {
	m.conn.Close()
}

func (m *grpcClientConn) GetServiceClient() interface{} {
	return m.serviceClient
}

// factory function in client connection pool
func (m *ClientGrpc) newConn(addr string) rpcClientConn {
	fun := "ClientGrpc.newConn-->"

	// 可加入多种拦截器
	tracer := opentracing.GlobalTracer()
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			otgrpc.OpenTracingClientInterceptor(tracer)),
		grpc.WithStreamInterceptor(
			otgrpc.OpenTracingStreamClientInterceptor(tracer)),
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		slog.Errorf("%s NetTSocket addr:%s err:%s", fun, addr, err)
		return nil
	}
	client := m.fnFactory(conn)
	return &grpcClientConn{
		serviceClient: client,
		conn:          conn,
	}
}
