package rocserv

import (
	"fmt"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"google.golang.org/grpc"
	"runtime"
	"strings"
	"sync"
	"time"
)

type ServProtocol int

const (
	GRPC ServProtocol = iota
	THRIFT
)

type ClientGrpc struct {
	clientLookup ClientLookup
	processor    string
	poolLen      int
	poolClient   sync.Map
	breaker      *Breaker
	router       Router
	fnFactory    func(client *grpc.ClientConn) interface{}
}

func NewClientGrpcWithRouterType(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}, routerType int) *ClientGrpc {
	return &ClientGrpc{
		clientLookup: cb,
		processor:    processor,
		poolLen:      poollen,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
		fnFactory:    fn,
	}
}

func NewClientGrpcByConcurrentRouter(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, poollen, fn, 1)
}

func NewClientGrpc(cb ClientLookup, processor string, poollen int, fn func(client *grpc.ClientConn) interface{}) *ClientGrpc {
	return NewClientGrpcWithRouterType(cb, processor, poollen, fn, 0)
}

func (m *ClientGrpc) Rpc(haskkey string, fnrpc func(interface{}) error) error {
	si, rc := m.route(haskkey)
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

	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, GRPC, nil)
	return err
}

func (m *ClientGrpc) route(key string) (*ServInfo, rpcClient) {
	fun := "ClientGrpc.route -->"

	s := m.router.Route(m.processor, key)
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

func (m *ClientGrpc) getPool(addr string) chan rpcClient {
	fun := "ClientGrpc.getPool -->"

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

func (m *ClientGrpc) newClient(addr string) rpcClient {
	fun := "ClientGrpc.newClient -->"

	// 可加入多种拦截器
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
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

func (m *ClientGrpc) rpc(si *ServInfo, rc rpcClient, fnrpc func(interface{}) error) error {
	fun := "ClientGrpc.rpc -->"

	//rc.SetTimeout(timeout)
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

func (m *ClientGrpc) Payback(si *ServInfo, client rpcClient) {
	fun := "ClientGrpc.Payback -->"

	// po 链接池
	po := m.getPool(si.Addr)
	select {

	// 回收连接 client
	case po <- client:
		slog.Tracef("%s payback:%s len:%d", fun, si, len(po))

	//不能回收了，关闭链接
	default:
		slog.Infof("%s full not payback:%s len:%d", fun, si, len(po))
		client.Close()
	}

	//m.printPool()
}
