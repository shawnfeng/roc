package rocserv

import (
	"sync"

	"github.com/shawnfeng/sutil/slog"
)

const (
	defaultPoolLen = 512
)

type ClientPool struct {
	poolClient sync.Map
	poolLen    int
	Factory    func(addr string) rpcClient
}

// NewClientPool constructor of pool, 如果连接数过低，修正为默认值
func NewClientPool(poolLen int, factory func(addr string) rpcClient) *ClientPool {
	if poolLen < defaultPoolLen {
		poolLen = defaultPoolLen
	}
	return &ClientPool{poolLen: poolLen, Factory: factory}
}

// Get get connection from pool, if reach max, create new connection and return
func (m *ClientPool) Get(addr string) rpcClient {
	fun := "ClientPool.Get -->"

	po := m.getPool(addr)
	var c rpcClient
	select {
	case c = <-po:
		slog.Tracef("%s get: %s len:%d", fun, addr, len(po))
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

// Put 连接池回收连接
func (m *ClientPool) Put(addr string, client rpcClient, err error) {
	fun := "ClientPool.Put -->"
	// do nothing，应该不会发生
	if client == nil {
		slog.Errorf("%s put nil rpc client to pool: %s", fun, addr)
		return
	}
	// close client and don't put to pool
	if err != nil {
		slog.Warnf("%s put rpc client to pool: %s, with err: %v", fun, addr, err)
		client.Close()
		return
	}

	// po 链接池
	po := m.getPool(addr)
	select {
	// 回收连接 client
	case po <- client:
		slog.Tracef("%s payback:%s len:%d", fun, addr, len(po))

	//不能回收了，关闭链接(满了)
	default:
		slog.Warnf("%s full not payback: %s len: %d", fun, addr, len(po))
		client.Close()
	}
}
