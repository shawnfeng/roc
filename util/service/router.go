// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"github.com/shawnfeng/sutil/slog"
	"sync"
)

func NewRouter(routerType int, cb ClientLookup) Router {
	fun := "NewRouter -->"

	switch routerType {
	case 0:
		return NewHash(cb)
	case 1:
		return NewConcurrent(cb)
	default:
		slog.Errorf("%s routerType err: %d", fun, routerType)
		return NewHash(cb)
	}
}

type Router interface {
	Route(processor, key string) *ServInfo
	Pre(s *ServInfo) error
	Post(s *ServInfo) error
}

func NewHash(cb ClientLookup) *Hash {
	return &Hash{
		cb: cb,
	}
}

type Hash struct {
	cb ClientLookup
}

func (m *Hash) Route(processor, key string) *ServInfo {
	return m.cb.GetServAddr(processor, key)
}

func (m *Hash) Pre(s *ServInfo) error {
	return nil
}

func (m *Hash) Post(s *ServInfo) error {
	return nil
}

func NewConcurrent(cb ClientLookup) *Concurrent {
	return &Concurrent{
		cb:      cb,
		counter: make(map[string]int64),
	}
}

type Concurrent struct {
	cb      ClientLookup
	mutex   sync.Mutex
	counter map[string]int64
}

func (m *Concurrent) Route(processor, key string) *ServInfo {
	list := m.cb.GetAllServAddr(processor)
	if list == nil {
		return nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	min := int64(0)
	var s *ServInfo
	for _, serv := range list {
		count := m.counter[serv.Addr]
		if min == 0 || min > count {
			min = count
			s = serv
		}
	}

	return s
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
