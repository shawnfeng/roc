package rocserv

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil/pool"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil/sync2"
)

const (
	getConnTimeout = time.Second * 1
	statTick       = time.Second * 10
)

var (
	ErrConnectionPoolClosed = errors.New("connection pool is closed")
)

type rpcClientConn interface {
	Close() error
	SetTimeout(timeout time.Duration) error
	GetServiceClient() interface{}
}

// ConnectionPool connection pool corresponding to the addr
type ConnectionPool struct {
	calleeServiceKey string

	rpcType     string
	rpcFactory  func(addr string) (rpcClientConn, error)
	addr        string
	idle        int // count of idle connection
	active      int // count of established connection, sometime active will bigger than idle, then the conn will be closed
	idleTimeout time.Duration

	mu          sync.Mutex
	closed      sync2.AtomicBool
	connections *pool.List // 对应地址的连接池
}

// NewConnectionPool constructor of ConnectionPool
func NewConnectionPool(addr string, idle, active int, idleTimeout time.Duration, rpcFactory func(addr string) (rpcClientConn, error), calleeServiceKey string) *ConnectionPool {
	cp := &ConnectionPool{addr: addr, idle: idle, active: active, idleTimeout: idleTimeout, rpcFactory: rpcFactory, calleeServiceKey: calleeServiceKey, closed: sync2.NewAtomicBool(false)}
	return cp
}

func (cp *ConnectionPool) Open() {
	if cp.idle == 0 {
		cp.idle = defaultMaxIdle
	}
	if cp.active == 0 {
		cp.active = defaultMaxActive
	}
	if cp.idle > cp.active {
		cp.idle = cp.active
	}
	if cp.active < defaultMaxActive {
		cp.active = defaultMaxActive
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.connections = pool.NewList(&pool.Config{Active: cp.active, Idle: cp.idle, IdleTimeout: xtime.Duration(cp.idleTimeout), WaitTimeout: xtime.Duration(time.Second), Wait: true})
	cp.connections.New = func(ctx context.Context) (io.Closer, error) {
		return cp.rpcFactory(cp.addr)
	}
	cp.stat()
	return
}

func (cp *ConnectionPool) stat() {
	tickC := time.Tick(statTick)
	go func() {
		for !cp.closed.Get() {
			select {
			case <-tickC:
				if cp.closed.Get() {
					return
				}
				confActive, confIdle, active, idle := cp.connections.Stat()
				xlog.Infof(context.Background(), "caller: %s, callee: %s, callee_addr: %s, conf_active: %d, conf_idle: %d, active: %d, idle: %d", GetServName(), cp.calleeServiceKey, cp.addr, confActive, confIdle, active, idle)
				group, service := GetGroupAndService()
				_metricRPCConnectionPool.With(xprom.LabelGroupName, group,
					xprom.LabelServiceName, service,
					xprom.LabelCalleeService, cp.calleeServiceKey,
					calleeAddr, cp.addr,
					connectionPoolStatType, confActiveType).Set(float64(confActive))
				_metricRPCConnectionPool.With(xprom.LabelGroupName, group,
					xprom.LabelServiceName, service,
					xprom.LabelCalleeService, cp.calleeServiceKey,
					calleeAddr, cp.addr,
					connectionPoolStatType, activeType).Set(float64(active))
				_metricRPCConnectionPool.With(xprom.LabelGroupName, group,
					xprom.LabelServiceName, service,
					xprom.LabelCalleeService, cp.calleeServiceKey,
					calleeAddr, cp.addr,
					connectionPoolStatType, confIdleType).Set(float64(confIdle))
				_metricRPCConnectionPool.With(xprom.LabelGroupName, group,
					xprom.LabelServiceName, service,
					xprom.LabelCalleeService, cp.calleeServiceKey,
					calleeAddr, cp.addr,
					connectionPoolStatType, idleType).Set(float64(idle))
			}
		}
		xlog.Infof(context.Background(), "caller: %s, callee: %s, callee_addr: %s exit stat", GetServName(), cp.calleeServiceKey, cp.addr)
	}()
}

func (cp *ConnectionPool) pool() (p *pool.List) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Close close connection pool
func (cp *ConnectionPool) Close() {
	cp.closed.Set(true)
	p := cp.pool()
	if p == nil {
		return
	}
	p.Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.mu.Unlock()
	return
}

// Addr return addr of connection pool
func (cp *ConnectionPool) Addr() string {
	return cp.addr
}

// Get return a connection, you should call PooledConnection's Recycle once done
func (cp *ConnectionPool) Get(ctx context.Context) (rpcClientConn, error) {
	p := cp.pool()
	if p == nil {
		return nil, ErrConnectionPoolClosed
	}

	getCtx, cancel := context.WithTimeout(ctx, getConnTimeout)
	defer cancel()
	r, err := p.Get(getCtx)
	if err != nil {
		return nil, err
	}

	return r.(rpcClientConn), nil
}

// Put recycle a connection into the pool
func (cp *ConnectionPool) Put(conn rpcClientConn, forceClose bool) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnectionPoolClosed)
	}
	p.Put(context.TODO(), conn, forceClose)
}
