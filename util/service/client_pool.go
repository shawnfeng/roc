package rocserv

import (
	"context"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
)

const (
	defaultMaxIdle     = 256 // 连接池里的最大连接数,超过的连接会被关闭
	defaultMaxActive   = 512 // 最大可建立连接数
	defaultIdleTimeout = time.Second * 120
)

// ClientPool every addr has a connection pool, each backend server has more than one addr, in client side, it's ClientPool
type ClientPool struct {
	calleeServiceKey string
	mu               sync.Mutex
	idle             int
	active           int
	idleTimeout      time.Duration
	clientPool       sync.Map
	rpcFactory       func(addr string) (RpcClientConn, error)
}

// NewClientPool constructor of pool, 如果连接数过低，修正为默认值
func NewClientPool(idle, active int, rpcFactory func(addr string) (RpcClientConn, error), calleeServiceKey string) *ClientPool {
	return &ClientPool{idle: idle, active: active, rpcFactory: rpcFactory, calleeServiceKey: calleeServiceKey, idleTimeout: defaultIdleTimeout}
}

// Get get connection from pool, if reach max, create new connection and return
func (m *ClientPool) Get(ctx context.Context, addr string) (RpcClientConn, error) {
	fun := "ClientPool.Get -->"
	cp := m.getPool(addr)
	ctx, cancel := context.WithTimeout(ctx, getConnTimeout)
	defer cancel()
	c, err := cp.Get(ctx)
	if err != nil {
		xlog.Errorf(ctx, "%s get conn from connection pool failed, callee_service: %s, addr: %s, err: %v", fun, m.calleeServiceKey, addr, err)
		return nil, err
	}
	return c.(RpcClientConn), nil
}

// Put 连接池回收连接
func (m *ClientPool) Put(addr string, client RpcClientConn, err error) {
	fun := "ClientPool.Put -->"
	cp := m.getPool(addr)
	// close client and don't put to pool
	if err != nil {
		xlog.Warnf(context.Background(), "%s put rpc client to pool with err: %v, callee_service: %s, addr: %s", fun, err, m.calleeServiceKey, addr)
		cp.Put(client, true)
		return
	}
	cp.Put(client, false)
}

// Close close connection pool in client pool
func (m *ClientPool) Close() {
	closeConnectionPool := func(key, value interface{}) bool {
		if connectionPool, ok := value.(*ConnectionPool); ok {
			connectionPool.Close()
		}
		return true
	}
	m.clientPool.Range(closeConnectionPool)
}

func (m *ClientPool) getPool(addr string) *ConnectionPool {
	fun := "ClientPool.getPool -->"
	var cp *ConnectionPool
	value, ok := m.clientPool.Load(addr)
	if ok == true {
		cp = value.(*ConnectionPool)
	} else {
		m.mu.Lock()
		defer m.mu.Unlock()
		value, ok := m.clientPool.Load(addr)
		if ok == true {
			cp = value.(*ConnectionPool)
		} else {
			xlog.Infof(context.Background(), "%s not found connection pool of callee_service: %s, addr: %s, create it", fun, m.calleeServiceKey, addr)
			cp = NewConnectionPool(addr, m.idle, m.active, m.idleTimeout, m.rpcFactory, m.calleeServiceKey)
			cp.Open()
			m.clientPool.Store(addr, cp)
		}
	}
	return cp
}
