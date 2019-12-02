package rocserv

import (
	"sync"
	"sync/atomic"
	"time"

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
	// if pool full, retry get 3 times, each time sleep 500ms
	i := 0
	for i < 3 {
		select {
		case c = <-po:
			slog.Tracef("%s get:%s len:%d, count:%d", fun, addr, len(po), atomic.LoadInt32(&m.count))
			return c
		default:
			if atomic.LoadInt32(&m.count) > int32(m.poolLen) {
				slog.Errorf("get client from addr: %s reach max: %d, retry: %d", addr, m.count, i)
				i++
				time.Sleep(time.Millisecond * 500)
			} else {
				c = m.Factory(addr)
				if c != nil {
					atomic.AddInt32(&m.count, 1)
				}
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

// Put 连接池回收连接
func (m *ClientPool) Put(addr string, client rpcClient, err error) {
	fun := "ClientPool.Put -->"
	// do nothing，应该不会发生
	if client == nil {
		slog.Errorf("%s put nil rpc client to pool: %s", fun, addr)
		return
	}
	// close client and don't put to pool but decr count
	if err != nil {
		slog.Errorf("%s put rpc client to pool: %s, with err: %v", fun, addr, err)
		client.Close()
		atomic.AddInt32(&m.count, -1)
	}

	// po 链接池
	po := m.getPool(addr)
	select {
	// 回收连接 client
	case po <- client:
		slog.Tracef("%s payback:%s len:%d, count:%d", fun, addr, len(po), atomic.LoadInt32(&m.count))

	//不能回收了，关闭链接(满了)
	default:
		slog.Infof("%s full not payback:%s len:%d, count:%d", fun, addr, len(po), atomic.LoadInt32(&m.count))
		atomic.AddInt32(&m.count, -1)
		client.Close()
	}
}
