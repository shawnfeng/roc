package rocserv

import (
	"context"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shawnfeng/sutil/slog"
	"github.com/uber/jaeger-client-go"
)

// ClientThrift client of thrift in adapter
type ClientThrift struct {
	clientLookup ClientLookup
	processor    string
	fnFactory    func(thrift.TTransport, thrift.TProtocolFactory) interface{}
	pool         *ClientPool
	breaker      *Breaker
	router       Router
}

func NewClientThrift(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, maxCapacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, maxCapacity, 0)
}

func NewClientThriftByConcurrentRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, maxCapacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, maxCapacity, 1)
}

func NewClientThriftByAddrRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, maxCapacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, maxCapacity, 2)
}

// NewClientThriftWithRouterType create thrift client with router type, fn: xxServiceClientFactory, such as NewServmgrServiceClientFactory
func NewClientThriftWithRouterType(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, maxCapacity, routerType int) *ClientThrift {
	ct := &ClientThrift{
		clientLookup: cb,
		processor:    processor,
		fnFactory:    fn,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
	}
	pool := NewClientPool(defaultCapacity, maxCapacity, ct.newConn, cb.ServKey())
	ct.pool = pool
	return ct
}

func (m *ClientThrift) route(ctx context.Context, key string) (*ServInfo, rpcClientConn) {
	s := m.router.Route(ctx, m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	conn, _ := m.pool.Get(addr)
	return s, conn
}

// deprecated
func (m *ClientThrift) Rpc(hashKey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	return m.RpcWithContext(context.Background(), hashKey, timeout, fnrpc)
}

// deprecated
func (m *ClientThrift) RpcWithContext(ctx context.Context, hashKey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	var err error
	funcName := GetFuncName(3)
	if funcName == "rpc" {
		funcName = GetFuncName(4)
	}
	retry := GetFuncRetry(m.clientLookup.ServKey(), funcName)
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)
	for ; retry >= 0; retry-- {
		err = m.do(ctx, hashKey, funcName, timeout, fnrpc)
		if err == nil {
			return nil
		}
	}
	return err
}

func (m *ClientThrift) do(ctx context.Context, hashKey, funcName string, timeout time.Duration, fnrpc func(interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.router.Pre(si)
	defer m.router.Post(si)

	// 目前Adapter内通过Rpc函数调用RpcWithContext时层次会出错，直接调用RpcWithContext和RpcWithContextV2的层次是正确的，所以修正前者进行兼容
	call := func(si *ServInfo, rc rpcClientConn, timeout time.Duration, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	var err error
	// record request duration
	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}

func (m *ClientThrift) RpcWithContextV2(ctx context.Context, hashKey string, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	var err error
	funcName := GetFuncNameWithCtx(ctx, 3)
	retry := GetFuncRetry(m.clientLookup.ServKey(), funcName)
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)
	for ; retry >= 0; retry-- {
		err = m.doWithContext(ctx, hashKey, funcName, timeout, fnrpc)
		if err == nil {
			return nil
		}
	}
	return err
}

func (m *ClientThrift) doWithContext(ctx context.Context, hashKey, funcName string, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.logTraffic(ctx, si)
	ctx = m.injectServInfo(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClientConn, timeout time.Duration, fnrpc func(context.Context, interface{}) error) func() error {
		return func() error {
			return m.rpcWithContext(ctx, si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	var err error
	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}
func (m *ClientThrift) rpc(si *ServInfo, rc rpcClientConn, timeout time.Duration, fnrpc func(interface{}) error) error {
	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(c)
	m.pool.Put(si.Addr, rc, err)
	return err
}

func (m *ClientThrift) rpcWithContext(ctx context.Context, si *ServInfo, rc rpcClientConn, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(ctx, c)
	m.pool.Put(si.Addr, rc, err)
	return err
}

func (m *ClientThrift) injectServInfo(ctx context.Context, si *ServInfo) context.Context {
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

func (m *ClientThrift) logTraffic(ctx context.Context, si *ServInfo) {
	kv := make(map[string]interface{})
	for k, v := range trafficKVFromContext(ctx) {
		kv[k] = v
	}

	kv[TrafficLogKeyServerType] = si.Type
	kv[TrafficLogKeyServerID] = si.Servid
	kv[TrafficLogKeyServerName] = serviceFromServPath(m.clientLookup.ServPath())
	logTrafficByKV(ctx, kv)
}

type thriftClientConn struct {
	tsock         *thrift.TSocket
	trans         thrift.TTransport
	serviceClient interface{}
}

func (m *thriftClientConn) SetTimeout(timeout time.Duration) error {
	return m.tsock.SetTimeout(timeout)
}

func (m *thriftClientConn) Close() {
	m.trans.Close()
}

func (m *thriftClientConn) GetServiceClient() interface{} {
	return m.serviceClient
}

func (m *ClientThrift) newConn(addr string) (rpcClientConn, error) {
	fun := "ClientThrift.newConn -->"

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		slog.Errorf("%s NetTSocket addr:%s serv:%s err:%s", fun, addr, m.clientLookup.ServKey(), err)
		return nil, err
	}
	useTransport := transportFactory.GetTransport(transport)

	if err := useTransport.Open(); err != nil {
		slog.Errorf("%s Open addr:%s serv:%s err:%s", fun, addr, m.clientLookup.ServKey(), err)
		return nil, err
	}
	// 必须要close么？
	//useTransport.Close()

	slog.Infof("%s new client addr:%s serv:%s", fun, addr, m.clientLookup.ServKey())
	return &thriftClientConn{
		tsock:         transport,
		trans:         useTransport,
		serviceClient: m.fnFactory(useTransport, protocolFactory),
	}, nil
}
