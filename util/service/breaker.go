// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"encoding/json"
	"fmt"
	"github.com/shawnfeng/hystrix-go/hystrix"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"sync"
	"time"
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

	st := stime.NewTimeStat()
	defer func() {
		dur := st.Duration()
		slog.Infof("%s servname:%s rawGlobalConf:%s rawServConf:%s dur:%d", fun, servname, rawGlobalConf, rawServConf, dur)
	}()

	if len(rawGlobalConf) > 0 && rawGlobalConf != m.getRawGlobalConf() {

		var globalConf []*ItemConf
		err := json.Unmarshal([]byte(rawGlobalConf), &globalConf)
		if err != nil {
			slog.Errorf("%s servname:%s Unmarshal err, rawGlobalConf:%s", fun, servname, rawGlobalConf)
		} else {
			m.setGlobalConf(globalConf)
			m.setRawGlobalConf(rawGlobalConf)
		}
	}

	if len(rawServConf) > 0 && rawServConf != m.getRawServConf() {
		var items []*ItemConf
		err := json.Unmarshal([]byte(rawServConf), &items)
		if err != nil {
			slog.Errorf("%s servname:%s Unmarshal err, rawServConf:%s", fun, servname, rawServConf)
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

type Breaker struct {
	conf     *BreakerConf
	servName string

	clientLookup ClientLookup
	useFuncConf  map[string]*ItemConf
	rwMutex      sync.RWMutex
}

func NewBreaker(clientLookup ClientLookup) *Breaker {
	hystrix.SetLogger(slog.GetLogger())
	m := &Breaker{
		useFuncConf:  make(map[string]*ItemConf),
		clientLookup: clientLookup,
		servName:     clientLookup.ServKey(),
		conf:         NewBreakerConf(),
	}

	go m.run()

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

func (m *Breaker) generateKey(source int32, servid int, funcName string) string {
	return fmt.Sprintf("%d.%s.%d.%s", source, m.servName, servid, funcName)
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

func (m *Breaker) checkOrUpdateConf(source int32, servid int, funcName string) (needBreaker bool) {
	fun := "Breaker.checkOrUpdateConf -->"

	key := m.generateKey(source, servid, funcName)

	newConf := m.conf.getFuncConf(source, funcName)
	slog.Infof("%s servid:%d funcName:%s key:%s, newConf:%v", fun, servid, funcName, key, *newConf)

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

func (m *Breaker) Do(source int32, servid int, funcName string, run func() error, fallback func(error) error) error {
	fun := "Breaker.Do -->"

	//st := stime.NewTimeStat()
	key := ""
	/*
		defer func() {
			dur := st.Duration()
			slog.Infof("%s servid:%d funcName:%s key:%s dur:%d", fun, servid, funcName, key, dur)
		}()
	*/

	if m.checkOrUpdateConf(source, servid, funcName) == false {
		return run()
	}

	key = m.generateKey(source, servid, funcName)

	err := hystrix.Do(key, run, fallback)
	if err != nil {
		slog.Errorf("%s key:%s err:%s", fun, key, err.Error())
	}

	return err
}
