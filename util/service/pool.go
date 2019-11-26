package rocserv

import (
	"sync"
	"sync/atomic"

	"github.com/shawnfeng/sutil/slog"
)

type ClientPool struct {
	poolClient sync.Map
	poolLen    int
	count      int32
	Factory    func(addr string) rpcClient
}

func NewClientPool(poolLen int, factory func(addr string) rpcClient) *ClientPool {
	return &ClientPool{poolLen: poolLen, Factory: factory, count: 0}
}

func (m *ClientPool) Get(addr string) rpcClient {
	fun := "ClientPool.Get -->"

	po := m.getPool(addr)
	var c rpcClient
	// if pool full, retry get 3 times
	i := 0
	for i < 3 {
		select {
		case c = <-po:
			slog.Tracef("%s get:%s len:%d", fun, addr, len(po))
			return c
		default:
			if atomic.LoadInt32(&m.count) > int32(m.poolLen) {
				slog.Errorf("get client from addr: %s reach max: %d, retry: %d", addr, m.count, i)
				i++
			} else {
				c = m.Factory(addr)
				return c
			}
		}
	}
	slog.Errorf("get client from addr: %s reach max: %d after retry 3 times", addr, m.count)
	return nil
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
		atomic.AddInt32(&m.count, -1)
		client.Close()
	}
}
