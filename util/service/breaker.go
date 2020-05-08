// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"

	"github.com/shawnfeng/hystrix-go/hystrix"
	"github.com/shawnfeng/sutil/slog"
)

type ItemConf struct {
	*hystrix.CommandConfig
	Name   string `json:"name"`
	Enable bool   `json:"enable"`
	Source int32  `json:"source"`
}

type BreakerConf struct {
	globalConf []*ItemConf
	servConf   []*ItemConf
	funcConf   map[string]*ItemConf
	rwMutex    sync.RWMutex

	rawServConf   string
	rawGlobalConf string
	rawConfMutex  sync.RWMutex
}

func NewBreakerConf() *BreakerConf {
	return &BreakerConf{}
}

func (m *BreakerConf) tryUpdate(servname, rawGlobalConf, rawServConf string) {
	fun := "BreakerConf.tryUpdate -->"
	ctx := context.Background()

	if len(rawGlobalConf) > 0 && rawGlobalConf != m.getRawGlobalConf() {
		var globalConf []*ItemConf
		err := json.Unmarshal([]byte(rawGlobalConf), &globalConf)
		if err != nil {
			xlog.Errorf(ctx, "%s servname:%s Unmarshal err, rawGlobalConf:%s", fun, servname, rawGlobalConf)
		} else {
			m.setGlobalConf(globalConf)
			m.setRawGlobalConf(rawGlobalConf)
		}
	}

	if len(rawServConf) > 0 && rawServConf != m.getRawServConf() {
		var items []*ItemConf
		err := json.Unmarshal([]byte(rawServConf), &items)
		if err != nil {
			xlog.Errorf(ctx, "%s servname:%s Unmarshal err, rawServConf:%s", fun, servname, rawServConf)
		} else {

			var servConf []*ItemConf
			funcConf := make(map[string]*ItemConf)
			for _, item := range items {
				if item.Name == "default" {
					servConf = append(servConf, item)
					continue
				}

				key := fmt.Sprintf("%d.%s", item.Source, item.Name)
				funcConf[key] = item
			}
			m.setServConf(servConf)
			m.setFuncConf(funcConf)

			m.setRawServConf(rawServConf)
		}
	}
}

func (m *BreakerConf) getRawServConf() string {
	m.rawConfMutex.RLock()
	defer m.rawConfMutex.RUnlock()

	return m.rawServConf
}

func (m *BreakerConf) getRawGlobalConf() string {
	m.rawConfMutex.RLock()
	defer m.rawConfMutex.RUnlock()

	return m.rawGlobalConf
}

func (m *BreakerConf) setRawGlobalConf(rawGlobalConf string) {
	m.rawConfMutex.Lock()
	defer m.rawConfMutex.Unlock()

	m.rawGlobalConf = rawGlobalConf
}

func (m *BreakerConf) setRawServConf(rawServConf string) {
	m.rawConfMutex.Lock()
	defer m.rawConfMutex.Unlock()

	m.rawServConf = rawServConf
}

func (m *BreakerConf) getFuncConf(source int32, funcName string) *ItemConf {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	key := fmt.Sprintf("%d.%s", source, funcName)
	if c, ok := m.funcConf[key]; ok {
		return c
	}

	if m.servConf != nil {
		for _, item := range m.servConf {
			if item.Source == source {
				return item
			}
		}
	}

	if m.globalConf != nil {
		for _, item := range m.globalConf {
			if item.Source == source {
				return item
			}
		}
	}

	return &ItemConf{
		CommandConfig: &hystrix.CommandConfig{},
		Enable:        false,
	}
}

func (m *BreakerConf) setGlobalConf(globalConf []*ItemConf) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.globalConf = globalConf
}

func (m *BreakerConf) setServConf(servConf []*ItemConf) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.servConf = servConf
}

func (m *BreakerConf) setFuncConf(funcConf map[string]*ItemConf) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.funcConf = funcConf
}

type BreakerStat struct {
	key   string
	fail  int64
	total int64
}

type Breaker struct {
	conf     *BreakerConf
	servName string

	clientLookup ClientLookup
	useFuncConf  map[string]*ItemConf
	rwMutex      sync.RWMutex

	statCounter map[string]*BreakerStat
	statChan    chan *BreakerStat
}

func NewBreaker(clientLookup ClientLookup) *Breaker {
	hystrix.SetLogger(slog.GetLogger())
	m := &Breaker{
		useFuncConf:  make(map[string]*ItemConf),
		clientLookup: clientLookup,
		servName:     clientLookup.ServKey(),
		conf:         NewBreakerConf(),
		statCounter:  make(map[string]*BreakerStat),
		statChan:     make(chan *BreakerStat, 1024*10),
	}

	go m.run()
	go m.monitor()

	return m
}

func (m *Breaker) run() {

	for {
		rawServConf := m.clientLookup.GetBreakerServConf()
		rawGlobalConf := m.clientLookup.GetBreakerGlobalConf()
		m.conf.tryUpdate(m.servName, rawGlobalConf, rawServConf)

		time.Sleep(time.Second * 30)
	}
}

func (m *Breaker) monitor() {
	fun := "Breaker.monitor -->"
	ctx := context.Background()

	ticker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-ticker.C:
			for _, stat := range m.statCounter {
				if stat.fail > 5 && stat.total > 5 &&
					(float64(stat.fail)/float64(stat.total)) > 0.02 {
					xlog.Errorf(ctx, "%s breaker stat, key:%s, total:%d, fail:%d", fun, stat.key, stat.total, stat.fail)
				}
			}

			m.statCounter = make(map[string]*BreakerStat)

		case stat := <-m.statChan:
			tmp := &BreakerStat{
				key: stat.key,
			}
			if item, ok := m.statCounter[stat.key]; ok {
				tmp = item
			}

			tmp.fail += stat.fail
			tmp.total += stat.total

			m.statCounter[tmp.key] = tmp
		}
	}
}

func (m *Breaker) doStat(key string, total, fail int64) {
	fun := "Breaker.doStat -->"

	stat := &BreakerStat{
		key:   key,
		total: total,
		fail:  fail,
	}

	select {
	case m.statChan <- stat:
	default:
		xlog.Errorf(context.Background(), "%s drop, key:%s, total:%d, fail:%d", fun, stat.key, stat.total, stat.fail)
	}
}

func (m *Breaker) generateKey(source int32, servid int, funcName string, protocol ServProtocol) string {
	switch protocol {
	case HTTP:
		return fmt.Sprintf("%d.%s.%d.%s.http", source, m.servName, servid, funcName)
	case GRPC:
		return fmt.Sprintf("%d.%s.%d.%s.grpc", source, m.servName, servid, funcName)
	default:
		return fmt.Sprintf("%d.%s.%d.%s.thrift", source, m.servName, servid, funcName)
	}
}

func (m *Breaker) getUseFuncConf(key string) (*ItemConf, bool) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	conf, ok := m.useFuncConf[key]
	return conf, ok
}

func (m *Breaker) setUseFuncConf(key string, conf *ItemConf) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.useFuncConf[key] = conf
}

// 定时检查更新，用一个标志位
func (m *Breaker) checkOrUpdateConf(source int32, servid int, funcName string, key string) (needBreaker bool) {
	//fun := "Breaker.checkOrUpdateConf -->"

	newConf := m.conf.getFuncConf(source, funcName)

	if newConf.Enable == false {
		return false
	}

	useConf, ok := m.getUseFuncConf(key)
	if ok == false ||
		useConf.Enable || newConf.Enable ||
		useConf.Timeout != newConf.Timeout ||
		useConf.SleepWindow != newConf.SleepWindow ||
		useConf.RequestVolumeThreshold != newConf.RequestVolumeThreshold ||
		useConf.ErrorPercentThreshold != newConf.ErrorPercentThreshold ||
		useConf.MaxConcurrentRequests != newConf.MaxConcurrentRequests {

		hystrix.ConfigureCommand(key, *newConf.CommandConfig)

		m.setUseFuncConf(key, newConf)
	}

	return true
}

func (m *Breaker) Do(source int32, servid int, funcName string, run func() error, protocol ServProtocol, fallback func(error) error) error {
	fun := "Breaker.Do -->"
	ctx := context.Background()
	key := m.generateKey(source, servid, funcName, protocol)
	if m.checkOrUpdateConf(source, servid, funcName, key) == false {
		return run()
	}

	fail := int64(0)
	err := hystrix.Do(key, run, fallback)
	if err != nil {
		xlog.Warnf(ctx, "%s key:%s err: %s", fun, key, err)
		fail = 1
	}

	xlog.Debugf(ctx, "Breaker key:%s fail:%d", key, fail)
	m.doStat(key, 1, fail)

	return err
}
