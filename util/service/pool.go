package rocserv

import (
	"github.com/shawnfeng/sutil/slog"
	"sync"
)

type ClientPool struct {
	poolClient sync.Map
	poolLen    int
	Factory    func(addr string) rpcClient
}

func NewClientPool(poolLen int, factory func(addr string) rpcClient) *ClientPool {
	return &ClientPool{poolLen: poolLen, Factory: factory}
}

func (m *ClientPool) Get(addr string) rpcClient {
	fun := "ClientPool.Get -->"

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
	fun := "ClientPool.Put -->"

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
