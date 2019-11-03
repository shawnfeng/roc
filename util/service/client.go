// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/scontext"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/smetric"
	"github.com/shawnfeng/sutil/stime"
	"github.com/uber/jaeger-client-go"
	"runtime"
	"strings"
	"time"
)

type ClientLookup interface {
	GetServAddr(processor, key string) *ServInfo
	GetServAddrWithServid(servid int, processor, key string) *ServInfo
	GetServAddrWithGroup(group string, processor, key string) *ServInfo
	GetAllServAddr(processor string) []*ServInfo
	GetAllServAddrWithGroup(group, processor string) []*ServInfo
	ServKey() string
	ServPath() string
	GetBreakerServConf() string
	GetBreakerGlobalConf() string
}

func NewClientLookup(etcdaddrs []string, baseLoc string, servlocation string) (*ClientEtcdV2, error) {
	return NewClientEtcdV2(configEtcd{etcdaddrs, baseLoc}, servlocation)
}

type ClientWrapper struct {
	clientLookup ClientLookup
	processor    string
	breaker      *Breaker
	router       Router
}

func NewClientWrapper(cb ClientLookup, processor string) *ClientWrapper {
	return NewClientWrapperWithRouterType(cb, processor, 0)
}

func NewClientWrapperByConcurrentRouter(cb ClientLookup, processor string) *ClientWrapper {
	return NewClientWrapperWithRouterType(cb, processor, 1)
}

func NewClientWrapperWithRouterType(cb ClientLookup, processor string, routerType int) *ClientWrapper {
	return &ClientWrapper{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
	}
}

func (m *ClientWrapper) Do(hashKey string, timeout time.Duration, run func(addr string, timeout time.Duration) error) error {
	fun := "ClientWrapper.Do -->"
	si := m.router.Route(context.TODO(), m.processor, hashKey)
	if si == nil {
		return fmt.Errorf("%s not find service:%s processor:%s", fun, m.clientLookup.ServPath(), m.processor)
	}
	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(addr string, timeout time.Duration) func() error {
		return func() error {
			return run(addr, timeout)
		}
	}(si.Addr, timeout)

	funcName := GetFunName(3)
	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, HTTP, nil)
	return err
}

func (m *ClientWrapper) Call(ctx context.Context, hashKey, funcName string, run func(addr string) error) error {
	fun := "ClientWrapper.Call -->"

	si := m.router.Route(ctx, m.processor, hashKey)
	if si == nil {
		return fmt.Errorf("%s not find service:%s processor:%s", fun, m.clientLookup.ServPath(), m.processor)
	}
	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(addr string) func() error {
		return func() error {
			return run(addr)
		}
	}(si.Addr)

	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, HTTP, nil)
	return err
}

func collector(servkey string, processor string, duration time.Duration, source int, servid int, funcName string, err interface{}) {
	servBase := GetServBase()
	instance := ""
	if servBase != nil {
		instance = servBase.Copyname()
	}
	smetric.CollectServ(instance, servkey, servid, processor, duration, source, funcName, err)
}

type ClientThrift struct {
	clientLookup ClientLookup
	processor    string
	fnFactory    func(thrift.TTransport, thrift.TProtocolFactory) interface{}
	pool         *ClientPool
	breaker      *Breaker
	router       Router
}

type rpcClient interface {
	Close() error
	SetTimeout(timeout time.Duration) error
	GetServiceClient() interface{}
}

type rpcClient1 struct {
	tsock         *thrift.TSocket
	trans         thrift.TTransport
	serviceClient interface{}
}

func (m *rpcClient1) SetTimeout(timeout time.Duration) error {
	return m.tsock.SetTimeout(timeout)
}

func (m *rpcClient1) Close() error {
	return m.trans.Close()
}

func (m *rpcClient1) GetServiceClient() interface{} {
	return m.serviceClient
}

func NewClientThrift(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, poollen, 0)
}

func NewClientThriftByConcurrentRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, poollen, 1)
}

func NewClientThriftByAddrRouter(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int) *ClientThrift {
	return NewClientThriftWithRouterType(cb, processor, fn, poollen, 2)
}

func NewClientThriftWithRouterType(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen, routerType int) *ClientThrift {

	ct := &ClientThrift{
		clientLookup: cb,
		processor:    processor,
		fnFactory:    fn,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
	}
	pool := NewClientPool(poollen, ct.newClient)
	ct.pool = pool
	return ct
}

func (m *ClientThrift) newClient(addr string) rpcClient {
	fun := "ClientThrift.newClient -->"

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		slog.Errorf("%s NetTSocket addr:%s serv:%s err:%s", fun, addr, m.clientLookup.ServKey(), err)
		return nil
	}
	useTransport := transportFactory.GetTransport(transport)

	if err := useTransport.Open(); err != nil {
		slog.Errorf("%s Open addr:%s serv:%s err:%s", fun, addr, m.clientLookup.ServKey(), err)
		return nil
	}
	// 必须要close么？
	//useTransport.Close()

	slog.Infof("%s new client addr:%s serv:%s", fun, addr, m.clientLookup.ServKey())
	return &rpcClient1{
		tsock:         transport,
		trans:         useTransport,
		serviceClient: m.fnFactory(useTransport, protocolFactory),
	}
}

func (m *ClientThrift) route(ctx context.Context, key string) (*ServInfo, rpcClient) {
	s := m.router.Route(ctx, m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	return s, m.pool.Get(addr)
}

// deprecated
func (m *ClientThrift) Rpc(hashKey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	return m.RpcWithContext(context.Background(), hashKey, timeout, fnrpc)
}

// deprecated
func (m *ClientThrift) RpcWithContext(ctx context.Context, hashKey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	funcName := GetFunName(3)
	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}

func (m *ClientThrift) RpcWithContextV2(ctx context.Context, hashKey string, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	ctx = m.injectServInfo(ctx, si)
	m.logTraffic(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(context.Context, interface{}) error) func() error {
		return func() error {
			return m.rpcWithContext(ctx, si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	funcName := GetFunName(3)
	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}

func (m *ClientThrift) rpc(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) error {
	fun := "ClientThrift.rpc -->"

	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(c)
	if err == nil {
		m.pool.Put(si.Addr, rc)
	} else {
		slog.Warnf("%s close thrift client s:%s", fun, si)
		rc.Close()
	}
	return err
}

func (m *ClientThrift) rpcWithContext(ctx context.Context, si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	fun := "ClientThrift.rpcWithContext -->"

	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(ctx, c)
	if err == nil {
		m.pool.Put(si.Addr, rc)
	} else {
		slog.Warnf("%s close thrift client s:%s", fun, si)
		rc.Close()
	}
	return err
}

func (m *ClientThrift) injectServInfo(ctx context.Context, si *ServInfo) context.Context {
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

func GetFunName(index int) string {
	funcName := ""
	pc, _, _, ok := runtime.Caller(index)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
		if index := strings.LastIndex(funcName, "."); index != -1 {
			if len(funcName) > index+1 {
				funcName = funcName[index+1:]
			}
		}
	}
	return funcName
}
