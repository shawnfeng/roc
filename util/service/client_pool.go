package rocserv

import (
	"context"
	"sync"
	"time"

	"github.com/shawnfeng/sutil/slog"
)

const (
	defaultCapacity = 512
)

// ClientPool every addr has a connection pool, each backend server has more than one addr, in client side, it's ClientPool
type ClientPool struct {
	capacity    int
	maxCapacity int
	idleTimeout time.Duration
	clientPool  sync.Map
	rpcFactory  func(addr string) (rpcClientConn, error)
}

// NewClientPool constructor of pool, 如果连接数过低，修正为默认值
func NewClientPool(capacity, maxCapacity int, rpcFactory func(addr string) (rpcClientConn, error)) *ClientPool {
	return &ClientPool{capacity: capacity, maxCapacity: maxCapacity, rpcFactory: rpcFactory}
}

// Get get connection from pool, if reach max, create new connection and return
func (m *ClientPool) Get(addr string) (rpcClientConn, error) {
	fun := "ClientPool.Get -->"
	cp := m.getPool(addr)
	ctx, cancel := context.WithTimeout(context.Background(), getConnTimeout)
	defer cancel()
	c, err := cp.Get(ctx)
	if err != nil {
		slog.Errorf("%s get conn from connection pool failed, addr: %s", fun, addr)
		return nil, err
	}
	return c.(rpcClientConn), nil
}

// Put 连接池回收连接
func (m *ClientPool) Put(addr string, client rpcClientConn, err error) {
	fun := "ClientPool.Put -->"
	cp := m.getPool(addr)
	// close client and don't put to pool
	if err != nil {
		slog.Warnf("%s put rpc client to pool: %s, with err: %v", fun, addr, err)
		client.Close()
		cp.Put(nil)
		return
	}
	cp.Put(client)
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
		slog.Infof("%s not found connection pool of addr: %s, create it", fun, addr)
		cp = NewConnectionPool(addr, m.capacity, m.maxCapacity, m.idleTimeout, m.rpcFactory)
		cp.Open()
		m.clientPool.Store(addr, cp)
	}
	return cp
}
