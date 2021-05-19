package rocserv

import (
	"context"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/uber/jaeger-client-go"
)

// ClientThrift client of thrift in adapter
type ClientThrift struct {
	fallbacks

	clientLookup ClientLookup
	processor    string
	fnFactory    func(thrift.TTransport, thrift.TProtocolFactory) interface{}
	pool         *ClientPool
	breaker      *Breaker
	router       Router
}

func NewClientThrift(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, capacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, capacity, 0)
}

func NewClientThriftByConcurrentRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, capacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, capacity, 1)
}

func NewClientThriftByAddrRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, capacity int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, capacity, 2)
}

// NewClientThriftWithRouterType create thrift client with router type, fn: xxServiceClientFactory, such as NewServmgrServiceClientFactory
func NewClientThriftWithRouterType(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, capacity, routerType int) *ClientThrift {
	ct := &ClientThrift{
		clientLookup: cb,
		processor:    processor,
		fnFactory:    fn,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
	}
	// 目前写死值，后期改为动态获取的方式
	pool := NewClientPool(defaultMaxIdle, defaultMaxActive, ct.newConn, cb.ServKey())
	ct.pool = pool
	cb.RegisterDeleteAddrHandler(ct.deleteAddrHandler)
	return ct
}

func (m *ClientThrift) deleteAddrHandler(addrs []string) {
	for _, addr := range addrs {
		deleteAddrFromConnPool(addr, m.pool)
	}
}

func (m *ClientThrift) route(ctx context.Context, key string) (*ServInfo, rpcClientConn) {
	s := m.router.Route(ctx, m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	conn, _ := m.pool.Get(ctx, addr)
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

	call := func(_ctx context.Context) error {
		return m.rpc(si, rc, timeout, fnrpc)
	}

	var err error
	// record request duration
	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(ctx, funcName, call, m.GetFallbackFunc(funcName))
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
	var err error
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}
	defer func() {
		m.pool.Put(si.Addr, rc, err)
	}()

	ctx = m.injectServInfo(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(ctx context.Context) error {
		return m.rpcWithContext(ctx, si, rc, timeout, fnrpc)
	}

	st := xtime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(ctx, funcName, call, m.GetFallbackFunc(funcName))
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
	return err
}

func (m *ClientThrift) injectServInfo(ctx context.Context, si *ServInfo) context.Context {
	ctx, _ = xcontext.SetControlCallerServerName(ctx, serviceFromServPath(m.clientLookup.ServPath()))

	ctx, _ = xcontext.SetControlCallerServerID(ctx, fmt.Sprint(si.Servid))

	span := xtrace.SpanFromContext(ctx)
	if span == nil {
		return ctx
	}
	// 传入自己的servName进去
	span.SetBaggageItem(BaggageCallerKey, server.sbase.Servname())

	if jaegerSpan, ok := span.(*jaeger.Span); ok {
		ctx, _ = xcontext.SetControlCallerMethod(ctx, jaegerSpan.OperationName())
	}

	return ctx
}

func deleteAddrFromConnPool(addr string, pool *ClientPool) {
	fun := "deleteAddrFromConnPool -->"
	xlog.Infof(context.Background(), "%s get change addr success", fun)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	value, ok := pool.clientPool.Load(addr)
	if !ok {
		return
	}
	clientPool, ok := value.(*ConnectionPool)
	if !ok {
		xlog.Warnf(context.Background(), "%s value to connection pool false, key: %s", fun, addr)
		return
	}
	pool.clientPool.Delete(addr)
	clientPool.Close()
	xlog.Infof(context.Background(), "%s close client pool success, addr: %s", fun, addr)
}

//func (m *ClientThrift) logTraffic(ctx context.Context, si *ServInfo) {
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

type thriftClientConn struct {
	tsock         *thrift.TSocket
	trans         thrift.TTransport
	serviceClient interface{}
}

func (m *thriftClientConn) SetTimeout(timeout time.Duration) error {
	return m.tsock.SetTimeout(timeout)
}

func (m *thriftClientConn) Close() error {
	if m.trans != nil {
		m.trans.Close()
	}
	if m.tsock != nil {
		m.tsock.Close()
	}
	return nil
}

func (m *thriftClientConn) GetServiceClient() interface{} {
	return m.serviceClient
}

func (m *ClientThrift) newConn(addr string) (rpcClientConn, error) {
	fun := "ClientThrift.newConn -->"
	ctx := context.Background()

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		xlog.Errorf(ctx, "%s NetTSocket addr: %s serv: %s err: %v", fun, addr, m.clientLookup.ServKey(), err)
		return nil, err
	}
	useTransport := transportFactory.GetTransport(transport)

	if err := useTransport.Open(); err != nil {
		xlog.Errorf(ctx, "%s Open addr: %s serv: %s err: %v", fun, addr, m.clientLookup.ServKey(), err)
		return nil, err
	}
	// 必须要close么？
	//useTransport.Close()

	xlog.Infof(ctx, "%s new client addr: %s serv: %s", fun, addr, m.clientLookup.ServKey())
	return &thriftClientConn{
		tsock:         transport,
		trans:         useTransport,
		serviceClient: m.fnFactory(useTransport, protocolFactory),
	}, nil
}
