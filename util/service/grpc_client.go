package rocserv

import (
	"context"
	"errors"
	"fmt"
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"google.golang.org/grpc"
	"time"
)

type ServProtocol int

const (
	GRPC ServProtocol = iota
	THRIFT
	HTTP
)

type ClientGrpc struct {
	clientLookup ClientLookup
	processor    string
	breaker      *Breaker
	router       Router
	pool         *ClientPool
	fnFactory    func(client *grpc.ClientConn) interface{}
}

type Provider struct {
	Ip   string
	Port uint16
}

func NewClientGrpcWithRouterType(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}, routerType int) *ClientGrpc {
	clientGrpc := &ClientGrpc{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
		fnFactory:    fn,
	}
	pool := NewClientPool(poollen, clientGrpc.newClient)
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

	call := func(si *ServInfo, rc rpcClient, fnrpc func(interface{}) error) func() error {
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

func (m *ClientGrpc) getClient(provider *Provider) (*ServInfo, rpcClient, error) {
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
	return serv, m.pool.Get(serv.Addr), nil
}

func (m *ClientGrpc) Rpc(ctx context.Context, hashKey string, fnrpc func(interface{}) error) error {

	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	logTrafficForClientGrpc(ctx, m, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClient, fnrpc func(interface{}) error) func() error {
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

func (m *ClientGrpc) RpcWithContext(ctx context.Context, hashKey string, fnrpc func(interface{}) error) error {
	return m.Rpc(ctx, hashKey, fnrpc)
}

func (m *ClientGrpc) rpc(si *ServInfo, rc rpcClient, fnrpc func(interface{}) error) error {
	fun := "ClientGrpc.rpc -->"
	c := rc.GetServiceClient()
	err := fnrpc(c)
	if err == nil {
		m.pool.Put(si.Addr, rc)
	} else {
		slog.Warnf("%s close rpcclient s:%s", fun, si)
		rc.Close()
	}
	return err
}

func (m *ClientGrpc) route(ctx context.Context, key string) (*ServInfo, rpcClient) {
	s := m.router.Route(ctx, m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	return s, m.pool.Get(addr)
}

type grpcClient struct {
	serviceClient interface{}
	conn          *grpc.ClientConn
}

func (m *grpcClient) SetTimeout(timeout time.Duration) error {
	return fmt.Errorf("SetTimeout is not support ")
}

func (m *grpcClient) Close() error {
	return m.conn.Close()
}

func (m *grpcClient) GetServiceClient() interface{} {
	return m.serviceClient
}

func (m *ClientGrpc) newClient(addr string) rpcClient {
	fun := "ClientGrpc.newClient -->"

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
	return &grpcClient{
		serviceClient: client,
		conn:          conn,
	}
}
