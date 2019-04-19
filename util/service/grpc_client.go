package rocserv

import (
	"fmt"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"google.golang.org/grpc"
	"sync"
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

	funcName := GetFunName(2)

	var err error
	st := stime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(0, si.Servid, funcName, call, GRPC, nil)
	return err
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

func (m *ClientGrpc) route(key string) (*ServInfo, rpcClient) {
	s := m.router.Route(m.processor, key)
	if s == nil {
		return nil, nil
	}
	addr := s.Addr
	return s, m.pool.GrtClient(addr)
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

type ClientPool struct {
	poolClient sync.Map
	poolLen    int
	Factory    func(addr string) rpcClient
}

func NewClientPool(poolLen int, factory func(addr string) rpcClient) *ClientPool {
	return &ClientPool{poolLen: poolLen, Factory: factory}
}

func (m *ClientPool) GrtClient(addr string) rpcClient {
	fun := "ClientPool.GrtClient -->"
	po := m.getPool(addr)

	var c rpcClient
	select {
	case c = <-po:
		slog.Tracef("%s get:%s len:%d", fun, addr, len(po))
	default:
		c = m.Factory(addr)
	}
	return c
}

func (m *ClientPool) getPool(addr string) chan rpcClient {
	fun := "ClientPool.getPool -->"
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

// 连接池链接回收
func (m *ClientPool) Put(addr string, client rpcClient) {
	fun := "ClientPool.Close -->"

	// po 链接池
	po := m.getPool(addr)
	select {

	// 回收连接 client
	case po <- client:
		slog.Tracef("%s payback:%s len:%d", fun, addr, len(po))

	//不能回收了，关闭链接(满了)
	default:
		slog.Infof("%s full not payback:%s len:%d", fun, addr, len(po))
		client.Close()
	}
}
