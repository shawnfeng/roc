// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"sync"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"

	"github.com/shawnfeng/sutil/slog"
)

// Router router include consistent hash、load of concurrent、concrete addr
type Router interface {
	Route(ctx context.Context, processor, key string) *ServInfo
	Pre(s *ServInfo) error
	Post(s *ServInfo) error
}

func NewRouter(routerType int, cb ClientLookup) Router {
	fun := "NewRouter -->"

	switch routerType {
	case 0:
		return NewHash(cb)
	case 1:
		return NewConcurrent(cb)
	case 2:
		return NewAddr(cb)
	default:
		slog.Errorf("%s routerType err: %d", fun, routerType)
		return NewHash(cb)
	}
}

type Hash struct {
	cb ClientLookup
}

func NewHash(cb ClientLookup) *Hash {
	return &Hash{
		cb: cb,
	}
}

func (m *Hash) Pre(s *ServInfo) error {
	return nil
}

func (m *Hash) Post(s *ServInfo) error {
	return nil
}

func (m *Hash) Route(ctx context.Context, processor, key string) *ServInfo {
	//fun := "Hash.Route -->"

	group := xcontext.GetControlRouteGroupWithDefault(ctx, xcontext.DefaultGroup)
	s := m.cb.GetServAddrWithGroup(group, processor, key)

	//slog.Infof("%s group:%s, processor:%s, key:%s, s:%v", fun, group, processor, key, s)
	return s
}

type Concurrent struct {
	cb      ClientLookup
	mutex   sync.Mutex
	counter map[string]int64
}

func NewConcurrent(cb ClientLookup) *Concurrent {
	return &Concurrent{
		cb:      cb,
		counter: make(map[string]int64),
	}
}

func (m *Concurrent) Pre(s *ServInfo) error {
	m.mutex.Lock()
	m.counter[s.Addr] += 1
	m.mutex.Unlock()

	return nil
}

func (m *Concurrent) Post(s *ServInfo) error {
	m.mutex.Lock()
	m.counter[s.Addr] -= 1
	m.mutex.Unlock()

	return nil
}

func (m *Concurrent) Route(ctx context.Context, processor, key string) *ServInfo {
	fun := "Concurrent.Route -->"

	group := xcontext.GetControlRouteGroupWithDefault(ctx, xcontext.DefaultGroup)
	s := m.route(group, processor, key)
	if s != nil {
		slog.Infof("%s group:%s, processor:%s, key:%s, s:%v", fun, group, processor, key, s)
		return s
	}

	s = m.route("", processor, key)
	//slog.Infof("%s group:%s, new group:%s, processor:%s, key:%s, s:%v", fun, group, "", processor, key, s)
	return s
}

func (m *Concurrent) route(group, processor, key string) *ServInfo {
	fun := "route -->"

	list := m.cb.GetAllServAddrWithGroup(group, processor)
	if list == nil {
		return nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	min := int64(0)
	var s *ServInfo
	for _, serv := range list {

		count := m.counter[serv.Addr]
		//slog.Infof("%s processor:%s, addr:%s, count: %d", fun, processor, serv.Addr, count)
		if count == 0 {
			min = count
			s = serv
			break
		}

		if min == 0 || min > count {
			min = count
			s = serv
		}
	}
	if s != nil {
		//slog.Infof("%s processor:%s, addr:%s", fun, processor, s.Addr)
	} else {
		slog.Errorf("%s processor:%s, route fail", fun, processor)
	}

	return s
}

type Addr struct {
	cb ClientLookup
}

func NewAddr(cb ClientLookup) *Addr {
	return &Addr{cb: cb}
}

func (m *Addr) Pre(s *ServInfo) error {
	return nil
}

func (m *Addr) Post(s *ServInfo) error {
	return nil
}

// NOTE: 这里复用 key，作为 addr，用途同样是从一堆服务实例中找到目标实例
func (m *Addr) Route(ctx context.Context, processor, addr string) (si *ServInfo) {
	fun := "Addr.Route -->"

	group := xcontext.GetControlRouteGroupWithDefault(ctx, xcontext.DefaultGroup)
	servList := m.cb.GetAllServAddrWithGroup(group, processor)

	if servList == nil {
		return
	}

	for _, serv := range servList {
		if serv.Addr == addr {
			si = serv
			break
		}
	}

	if si != nil {
		slog.Infof("%s processor:%s, addr:%s", fun, processor, addr)
	} else {
		slog.Errorf("%s processor:%s, route failed", fun, processor)
	}

	return
}
