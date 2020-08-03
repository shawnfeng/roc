// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
)

// ClientWrapper 目前网关通过common/go/pub在使用
type ClientWrapper struct {
	fallbacks

	clientLookup ClientLookup
	processor    string
	breaker      *Breaker
	router       Router
}

func NewClientWrapper(cb ClientLookup, processor string) *ClientWrapper {
	return NewClientWrapperWithRouterType(cb, processor, 0)
}

func NewClientWrapperByConcurrentRouter(cb ClientLookup, processor string) *ClientWrapper {
	return NewClientWrapperWithRouterType(cb, processor, 1)
}

func NewClientWrapperWithRouterType(cb ClientLookup, processor string, routerType int) *ClientWrapper {
	return &ClientWrapper{
		clientLookup: cb,
		processor:    processor,
		breaker:      NewBreaker(cb),
		router:       NewRouter(routerType, cb),
	}
}

func (m *ClientWrapper) Do(hashKey string, timeout time.Duration, run func(addr string, timeout time.Duration) error) error {
	var err error
	funcName := GetFuncName(3)
	retry := GetFuncRetry(m.clientLookup.ServKey(), funcName)
	timeout = GetFuncTimeout(m.clientLookup.ServKey(), funcName, timeout)
	for ; retry >= 0; retry-- {
		err = m.do(hashKey, funcName, timeout, run)
		if err == nil {
			return nil
		}
	}
	return err
}

func (m *ClientWrapper) do(hashKey, funcName string, timeout time.Duration, run func(addr string, timeout time.Duration) error) error {
	fun := "ClientWrapper.Do -->"
	si := m.router.Route(context.TODO(), m.processor, hashKey)
	if si == nil {
		return fmt.Errorf("%s not find service:%s processor:%s", fun, m.clientLookup.ServPath(), m.processor)
	}
	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(_ctx context.Context) error {
		return run(si.Addr, timeout)
	}

	var err error
	st := xtime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(context.Background(), funcName, call, m.GetFallbackFunc(funcName))
	return err
}

func (m *ClientWrapper) Call(ctx context.Context, hashKey, funcName string, run func(addr string) error) error {
	fun := "ClientWrapper.Call -->"

	si := m.router.Route(ctx, m.processor, hashKey)
	if si == nil {
		return fmt.Errorf("%s not find service:%s processor:%s", fun, m.clientLookup.ServPath(), m.processor)
	}
	m.router.Pre(si)
	defer m.router.Post(si)

	call := func(_ctx context.Context) error {
		return run(si.Addr)
	}

	var err error
	st := xtime.NewTimeStat()
	defer func() {
		collector(m.clientLookup.ServKey(), m.processor, st.Duration(), 0, si.Servid, funcName, err)
	}()
	err = m.breaker.Do(ctx, funcName, call, m.GetFallbackFunc(funcName))
	return err
}
