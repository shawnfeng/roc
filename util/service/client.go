// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shawnfeng/sutil/slog"
	"runtime"
	"strings"
	"sync"
	"time"
)

type ClientLookup interface {
	GetServAddr(processor, key string) *ServInfo
	GetServAddrWithServid(servid int, processor, key string) *ServInfo
	//GetAllServAddr() map[string][]*ServInfo
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
}

func NewClientWrapper(cb ClientLookup, processor string) *ClientWrapper {
	return &ClientWrapper{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
	}
}

func (m *ClientWrapper) Do(haskkey string, timeout time.Duration, run func(addr string, timeout time.Duration) error) error {
	fun := "ClientWrapper.Do -->"
	si := m.clientLookup.GetServAddr(m.processor, haskkey)
	if si == nil {
		return fmt.Errorf("%s not find service:%s processor:%s", fun, m.clientLookup.ServPath(), m.processor)
	}

	call := func(addr string, timeout time.Duration) func() error {
		return func() error {
			return run(addr, timeout)
		}
	}(si.Addr, timeout)

	funcName := ""
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
		if index := strings.LastIndex(funcName, "."); index != -1 {
			if len(funcName) > index+1 {
				funcName = funcName[index+1:]
			}
		}
	}

	return m.breaker.Do(0, si.Servid, funcName, call, nil)
}

type ClientThrift struct {
	clientLookup ClientLookup
	processor    string
	fnFactory    func(thrift.TTransport, thrift.TProtocolFactory) interface{}

	poolLen int
	trace   bool

	poolClient sync.Map

	breaker *Breaker
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
	return NewClientThriftTraceFlag(cb, processor, fn, poollen, false)
}

func NewClientThriftTrace(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int) *ClientThrift {
	return NewClientThriftTraceFlag(cb, processor, fn, poollen, true)
}

func NewClientThriftTraceFlag(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int, trace bool) *ClientThrift {

	ct := &ClientThrift{
		clientLookup: cb,
		processor:    processor,
		fnFactory:    fn,
		poolLen:      poollen,
		trace:        trace,
		breaker:      NewBreaker(cb),
	}

	return ct
}

func (m *ClientThrift) newClient(addr string) rpcClient {
	fun := "ClientThrift.newClient -->"

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		slog.Errorf("%s NetTSocket addr:%s err:%s", fun, addr, err)
		return nil
	}
	useTransport := transportFactory.GetTransport(transport)

	if err := useTransport.Open(); err != nil {
		slog.Errorf("%s Open addr:%s err:%s", fun, addr, err)
		return nil
	}
	// 必须要close么？
	//useTransport.Close()

	slog.Infof("%s new client addr:%s", fun, addr)
	return &rpcClient1{
		tsock:         transport,
		trans:         useTransport,
		serviceClient: m.fnFactory(useTransport, protocolFactory),
	}

}

func (m *ClientThrift) getPool(addr string) chan rpcClient {
	fun := "ClientThrift.getPool -->"

	var tmp chan rpcClient
	value, ok := m.poolClient.Load(addr)
	if ok == true {
		tmp = value.(chan rpcClient)
	} else {
		slog.Infof("%s not found addr:%s", fun, addr)
		tmp = make(chan rpcClient, m.poolLen)
		m.poolClient.Store(addr, tmp)
	}

	return tmp

}

func (m *ClientThrift) hash(key string) (*ServInfo, rpcClient) {
	fun := "ClientThrift.hash -->"

	s := m.clientLookup.GetServAddr(m.processor, key)
	if s == nil {
		return nil, nil
	}

	addr := s.Addr
	po := m.getPool(addr)

	var c rpcClient
	select {
	case c = <-po:
		slog.Tracef("%s get:%s len:%d", fun, addr, len(po))
	default:
		c = m.newClient(addr)
	}

	//m.printPool()
	return s, c
}

func (m *ClientThrift) Payback(si *ServInfo, client rpcClient) {
	fun := "ClientThrift.Payback -->"

	po := m.getPool(si.Addr)
	select {
	case po <- client:
		slog.Tracef("%s payback:%s len:%d", fun, si, len(po))
	default:
		slog.Infof("%s full not payback:%s len:%d", fun, si, len(po))
		client.Close()
	}

	//m.printPool()
}

func (m *ClientThrift) Rpc(haskkey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	fun := "ClientThrift.Rpc -->"
	si, rc := m.hash(haskkey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}
	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(c)
	if err == nil {
		m.Payback(si, rc)
	} else {
		slog.Warnf("%s close rpcclient s:%s", fun, si)
		rc.Close()
	}
	return err
}

func (m *ClientThrift) RpcWithBreaker(haskkey string, timeout time.Duration, fnrpc func(interface{}) error) error {
	//fun := "ClientThrift.RpcWithBreaker -->"

	si, rc := m.hash(haskkey)
	if rc == nil {
		return fmt.Errorf("not find thrift service:%s processor:%s", m.clientLookup.ServPath(), m.processor)
	}

	call := func(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) func() error {
		return func() error {
			return m.rpc(si, rc, timeout, fnrpc)
		}
	}(si, rc, timeout, fnrpc)

	funcName := ""
	pc, _, _, ok := runtime.Caller(2)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
		if index := strings.LastIndex(funcName, "."); index != -1 {
			if len(funcName) > index+1 {
				funcName = funcName[index+1:]
			}
		}
	}

	return m.breaker.Do(0, si.Servid, funcName, call, nil)
}

func (m *ClientThrift) rpc(si *ServInfo, rc rpcClient, timeout time.Duration, fnrpc func(interface{}) error) error {
	fun := "ClientThrift.rpc -->"

	rc.SetTimeout(timeout)
	c := rc.GetServiceClient()

	err := fnrpc(c)
	if err == nil {
		m.Payback(si, rc)
	} else {
		slog.Warnf("%s close rpcclient s:%s", fun, si)
		rc.Close()
	}
	return err
}
