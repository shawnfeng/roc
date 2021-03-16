// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"sync"

	"gitlab.pri.ibanyu.com/middleware/dolphin/circuit_breaker"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
)

type ItemConf struct {
	Name   string `json:"name"`
	Enable bool   `json:"enable"`
	Source int32  `json:"source"`
}

type BreakerStat struct {
	key   string
	fail  int64
	total int64
}

type Breaker struct {
	servName     string // 形如 {servGroup}/{servName}
	clientLookup ClientLookup
	rwMutex      sync.RWMutex
}

func NewBreaker(clientLookup ClientLookup) *Breaker {
	m := &Breaker{
		clientLookup: clientLookup,
		servName:     clientLookup.ServKey(),
	}
	return m
}

func (m *Breaker) Do(ctx context.Context, funcName string, run func(ctx context.Context) error, fallback func(context.Context, error) error) error {
	fun := "Breaker.Do -->"
	fail := int64(0)
	// 稳定性平台，接口熔断配置的 key 形如 {servGroup}/{servName}/{funcName}。这里 m.servName 已为 {servGroup}/{servName} 形式了。
	key := m.servName + "/" + funcName
	err := circuit_breaker.Do(ctx, key, run, fallback)
	if err == circuit_breaker.ErrCircuitBreakerRegistryNotInited {
		// circuit_breaker 未初始化，视同无熔断。
		xlog.Warnf(ctx, " circuit breaker registry not inited! Call `circuit_breaker.Init()` in your project's `logic.Init()` first!")
		return run(ctx)
	}

	if err != nil {
		xlog.Warnf(ctx, "%s key:%s err: %s", fun, key, err)
		fail = 1
	}

	xlog.Debugf(ctx, "Breaker key:%s fail:%d", key, fail)
	return err
}

// fallbackFunc register utility. will be used by ClientGrpc, ClientThrift and ClientWrapper.
type fallbackFunc func(context.Context, error) error
type fallbacks struct {
	// funcName => fallbackFunc
	mp sync.Map
}

func (fs *fallbacks) RegisterFallbackFunc(name string, fallback fallbackFunc) {
	fs.mp.Store(name, fallback)
}

// `nil` will be returned if no fallback func is registered.
func (fs *fallbacks) GetFallbackFunc(name string) fallbackFunc {
	fallback, ok := fs.mp.Load(name)
	if !ok {
		return nil
	}
	return fallback.(fallbackFunc)
}
