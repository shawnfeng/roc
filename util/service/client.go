// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/sutil/scontext"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"github.com/uber/jaeger-client-go"

	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
)

const (
	// Timeout timeout(ms)
	Timeout = "timeoutMsec"
	// Default ...
	Default = "default"
	// DefaultTimeout ...
	DefaultTimeout = 3000 * time.Millisecond
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

	funcName := GetFunName(3)
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)
	call := func(addr string, timeout time.Duration) func() error {
		return func() error {
			return run(addr, timeout)
		}
	}(si.Addr, timeout)

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
	servidVal := strconv.Itoa(servid)
	sourceVal := strconv.Itoa(source)
	// record request duration to prometheus
	_metricRequestDuration.With(
		xprom.LabelServiceName, servkey,
		xprom.LabelServiceID, servidVal,
		xprom.LabelInstance, instance,
		xprom.LabelAPI, funcName,
		xprom.LabelSource, sourceVal,
		xprom.LabelType, processor).Observe(duration.Seconds())
	statusVal := "1"
	if err != nil {
		statusVal = "0"
	}
	_metricRequestTotal.With(
		xprom.LabelServiceName, servkey,
		xprom.LabelServiceID, servidVal,
		xprom.LabelInstance, instance,
		xprom.LabelAPI, funcName,
		xprom.LabelSource, sourceVal,
		xprom.LabelType, processor,
		labelStatus, statusVal).Inc()
}

func collectAPM(ctx context.Context, calleeService, calleeEndpoint string, servID int, duration time.Duration, requestErr error) {
	fun := "collectAPM -->"
	callerService := GetServName()

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		slog.Infof("%s span not found", fun)
		return
	}

	var callerEndpoint string
	if jspan, ok := span.(*jaeger.Span); ok {
		callerEndpoint = jspan.OperationName()
	} else {
		slog.Infof("%s unsupported span %v", fun, span)
		return
	}

	callerServiceID := strconv.Itoa(servID)
	_collectAPM(callerService, calleeService, callerEndpoint, calleeEndpoint, callerServiceID, duration, requestErr)
}

func _collectAPM(callerService, calleeService, callerEndpoint, calleeEndpoint, callerServiceID string, duration time.Duration, requestErr error) {
	_metricAPMRequestDuration.With(
		xprom.LabelCallerService, callerService,
		xprom.LabelCalleeService, calleeService,
		xprom.LabelCallerEndpoint, callerEndpoint,
		xprom.LabelCalleeEndpoint, calleeEndpoint,
		xprom.LabelCallerServiceID, callerServiceID).Observe(duration.Seconds())

	var status = "1"
	if requestErr != nil {
		status = "0"
	}

	_metricAPMRequestTotal.With(
		xprom.LabelCallerService, callerService,
		xprom.LabelCalleeService, calleeService,
		xprom.LabelCallerEndpoint, callerEndpoint,
		xprom.LabelCalleeEndpoint, calleeEndpoint,
		xprom.LabelCallerServiceID, callerServiceID,
		xprom.LabelCallStatus, status).Inc()
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

	// 目前Adapter内通过Rpc函数调用RpcWithContext时层次会出错，直接调用RpcWithContext和RpcWithContextV2的层次是正确的，所以修正前者进行兼容
	funcName := GetFunName(3)
	if funcName == "rpc" {
		funcName = GetFunName(4)
	}
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)

	call := func(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	var err error
	// record request duration
	st := stime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}

func (m *ClientThrift) RpcWithContextV2(ctx context.Context, hashKey string, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	si, rc := m.route(ctx, hashKey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	m.logTraffic(ctx, si)
	ctx = m.injectServInfo(ctx, si)

	m.router.Pre(si)
	defer m.router.Post(si)

	funcName := GetFunName(3)
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)
	call := func(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(context.Context, interface{}) error) func() error {
		return func() error {
			return m.rpcWithContext(ctx, si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	var err error
	st := stime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		collector(m.clientLookup.ServKey(), m.processor, dur, 0, si.Servid, funcName, err)
		collectAPM(ctx, m.clientLookup.ServKey(), funcName, si.Servid, dur, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, THRIFT, nil)
	return err
}

func (m *ClientThrift) rpc(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) error {
	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(c)
	m.pool.Put(si.Addr, rc, err)
	return err
}

func (m *ClientThrift) rpcWithContext(ctx context.Context, si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(context.Context, interface{}) error) error {
	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(ctx, c)
	m.pool.Put(si.Addr, rc, err)
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

// GetFuncTimeout get func timeout conf
func GetFuncTimeout(servKey, funcName string, timeout time.Duration) time.Duration {
	key := xutil.Concat(servKey, ".", funcName, ".", Timeout)
	var t int
	var exist bool
	if t, exist = GetApolloCenter().GetIntWithNamespace(context.TODO(), RPCConfNamespace, key); !exist {
		defaultKey := xutil.Concat(servKey, ".", Default, ".", Timeout)
		t, _ = GetApolloCenter().GetIntWithNamespace(context.TODO(), RPCConfNamespace, defaultKey)
	}
	if t == 0 {
		return timeout
	}

	return time.Duration(t) * time.Millisecond
}
