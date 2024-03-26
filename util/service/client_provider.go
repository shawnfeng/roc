// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// 包含服务注册、服务发现相关的定义、实现
package rocserv

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xconsistent"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
)

//动态提供服务信息
type ClientProvider struct {
	provider ServProvider
	servKey  string // 形如 {servGroup}/{servName}
	servPath string
	// 使用的注册器位置，不同版本会注册到不同版本的dist目录
	// 但是会保持多版本的兼容，客户端优先使用最新版本的
	distLoc string
	// 缓存地址列表，按照service id 降序的顺序存储
	// 按照processor 进行分组

	muServlist sync.Mutex
	servCopy   servCopyCollect
	servHash   map[string]*xconsistent.Consistent

	muChangeEvent      sync.Mutex
	changeEventHandler []deleteAddrHandler
}

type ProviderServInfo struct {
	ID        int                 `json:"id"`
	Protocols []*ProviderProtocol `json:"protocols"`
	Lane      string              `json:"lane"`
	Weight    int                 `json:"weight"`
	Disable   bool                `json:"disable"`
	Groups    []string            `json:"groups"`
}
type ProviderProtocol struct {
	Protocol ServProtocol `json:"protocol"`
	Addr     string       `json:"addr"`
}
type ServProvider interface {
	GetServInfos(ctx context.Context) []*ProviderServInfo
}

//func checkDistVersion(client etcd.KeysAPI, prefloc, servlocation string) string {
//	fun := "checkDistVersion -->"
//
//	return BASE_LOC_DIST_V2
//}

func NewClientProvider(provider ServProvider, servlocation string) (*ClientProvider, error) {
	//fun := "NewClientProvider -->"

	cli := &ClientProvider{
		provider: provider,
		servKey:  servlocation,
		distLoc:  BASE_LOC_DIST_V2,
		servPath: fmt.Sprintf("%s/%s", BASE_LOC_DIST_V2, servlocation),
	}
	cli.startWatch()
	return cli, nil
}

func (m *ClientProvider) startWatch() {
	ctx := context.Background()
	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-ticker.C:
				m.upServs(m.provider.GetServInfos(ctx))
			}
		}
	}()
}

func (m *ClientProvider) upServs(servs []*ProviderServInfo) {
	servCopy := make(servCopyCollect)
	for _, serv := range servs {
		sMap := make(map[string]*ServInfo)
		for _, p := range serv.Protocols {
			var sInfo = &ServInfo{
				Addr:   p.Addr,
				Servid: serv.ID,
			}
			switch p.Protocol {
			case GRPC:
				sInfo.Type = "grpc"
			case THRIFT:
				sInfo.Type = "thrift"
			default:
				return
			}
			sMap["proc_grpc"] = sInfo
		}
		servCopy[serv.ID] = &servCopyData{
			servId: serv.ID,
			// WARNING: v1版本不支持新版泳道元信息获取
			reg: &RegData{
				Servs: sMap,
				Lane:  &serv.Lane,
			},
			manual: &ManualData{
				Ctrl: &ServCtrl{
					Weight:  serv.Weight,
					Disable: serv.Disable,
					Groups:  serv.Groups,
				},
			},
		}
	}
	m.compareAndApplyCopyData(servCopy)
	m.upServlist(servCopy)
}

func (m *ClientProvider) upServlist(scopy map[int]*servCopyData) {
	fun := "ClientProvider.upServlist -->"
	ctx := context.Background()

	slist := make(map[string][]string)
	for sid, c := range scopy {
		if c == nil {
			xlog.Infof(ctx, "%s not found copy path:%s sid:%d", fun, m.servPath, sid)
			continue
		}

		if c.reg == nil {
			xlog.Infof(ctx, "%s not found regdata path:%s sid:%d", fun, m.servPath, sid)
			continue
		}

		if len(c.reg.Servs) == 0 {
			xlog.Infof(ctx, "%s not found servs path:%s sid:%d", fun, m.servPath, sid)
			continue
		}

		if c.manual == nil || c.manual.Ctrl == nil {
			continue
		}

		if c.manual.Ctrl.Disable {
			xlog.Infof(ctx, "%s disable path:%s sid:%d", fun, m.servPath, sid)
			continue
		}

		var weight = c.manual.Ctrl.Weight
		if weight == 0 {
			weight = 100
		}

		// 设置泳道实例列表, 兼容新老版本
		lane, ok := c.reg.GetLane()
		if ok {
			// 如果lane不为nil, 说明服务端已注册新版本lane元数据, 使用新版本更新泳道实例路由表
			xlog.Debugf(ctx, "%v use v2 lane metadata, lane: %v, servKey: %s, servPath: %s, sid: %d", fun, lane, m.servKey, m.servPath, c.servId)
			var tmpList []string
			if _, ok2 := slist[lane]; ok2 {
				tmpList = slist[lane]
			}
			for i := 0; i < weight; i++ {
				tmpList = append(tmpList, fmt.Sprintf("%d-%d", sid, i))
			}
			slist[lane] = tmpList
			continue
		}

		// 否则, 说明服务端还是老版本lane元数据 (在manual中), 退回老版本更新泳道路由表
		var tmpList []string
		for _, g := range c.manual.Ctrl.Groups {
			xlog.Debugf(ctx, "%v use v1 lane metadata, lane: %v, servKey: %s, servPath: %s, sid: %d", fun, g, m.servKey, m.servPath, c.servId)
			if _, ok := slist[g]; ok {
				tmpList = slist[g]
			}
			for i := 0; i < weight; i++ {
				tmpList = append(tmpList, fmt.Sprintf("%d-%d", sid, i))
			}

			slist[g] = tmpList
		}
	}

	shash := make(map[string]*xconsistent.Consistent)
	for group, list := range slist {
		hash := xconsistent.NewWithElts(list)
		if hash != nil {
			shash[group] = hash
		}
	}

	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	m.servHash = shash
	m.servCopy = scopy
	return
}

func (m *ClientProvider) GetServAddr(processor, key string) *ServInfo {
	//fun := "ClientProvider.GetServAddr -->"
	return m.GetServAddrWithGroup("", processor, key)
}

func (m *ClientProvider) GetServAddrWithGroup(group string, processor, key string) *ServInfo {
	fun := "ClientProvider.GetServAddrWithGroup-->"
	ctx := context.Background()
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	if m.servHash == nil {
		xlog.Errorf(ctx, "%s m.servHash == nil, serv path:%s hash circle processor:%s key:%s", fun, m.servPath, processor, key)
		return nil
	}

	if m.servHash[""] == nil {
		xlog.Errorf(ctx, "%s m.servHash[\"\"] == nil, serv path:%s hash circle processor:%s key:%s", fun, m.servPath, processor, key)
		return nil
	}

	shash, _ := m.servHash[group]
	if shash == nil {
		shash = m.servHash[""]
	}

	s, err := shash.Get(key)
	if err != nil {
		xlog.Errorf(ctx, "%s get serv path: %s processor: %s key: %s err: %v", fun, m.servPath, processor, key, err)
		return nil
	}

	idx := strings.Index(s, "-")
	if idx == -1 {
		xlog.Fatalf(ctx, "%s servid path: %s, processor: %s, key: %s, sid: %s", fun, m.servPath, processor, key, s)
		return nil
	}

	sid, err := strconv.Atoi(s[:idx])
	if err != nil || sid < 0 {
		xlog.Fatalf(ctx, "%s servid path:%s processor:%s key: %s, sid: %s, id: %d, err: %v", fun, m.servPath, processor, key, s, sid, err)
		return nil
	}
	return m.getServAddrWithServid(sid, processor, key)
}

func (m *ClientProvider) getServAddrWithServid(servid int, processor, key string) *ServInfo {
	if c := m.servCopy[servid]; c != nil {
		if c.reg != nil {
			if c.manual != nil && c.manual.Ctrl != nil && c.manual.Ctrl.Disable {
				return nil
			}
			if p := c.reg.Servs[processor]; p != nil {
				return p
			}
		}
	}

	return nil
}

func (m *ClientProvider) GetServAddrWithServid(servid int, processor, key string) *ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	return m.getServAddrWithServid(servid, processor, key)
}

func (m *ClientProvider) GetAllServAddr(processor string) []*ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	servs := make([]*ServInfo, 0)
	for _, c := range m.servCopy {
		if c.reg != nil {
			if c.manual != nil && c.manual.Ctrl != nil && c.manual.Ctrl.Disable {
				continue
			}
			if p := c.reg.Servs[processor]; p != nil {
				servs = append(servs, p)
			}
		}
	}

	return servs
}

func (m *ClientProvider) GetAllServAddrWithGroup(group, processor string) []*ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	var servs []*ServInfo
	for _, c := range m.servCopy {
		if c.reg != nil {
			if c.manual != nil && c.manual.Ctrl != nil && c.manual.Ctrl.Disable {
				continue
			}

			if !c.containsLane(group) {
				continue
			}

			if p := c.reg.Servs[processor]; p != nil {
				servs = append(servs, p)
			}
		}
	}

	return servs
}

func (m *ClientProvider) ServKey() string {
	return m.servKey
}

func (m *ClientProvider) ServPath() string {
	return m.servPath
}

func (m *ClientProvider) String() string {
	return fmt.Sprintf("service_key: %s, service_path: %s", m.servKey, m.servPath)
}

func (m *ClientProvider) RegisterDeleteAddrHandler(handle deleteAddrHandler) {
	m.muChangeEvent.Lock()
	defer m.muChangeEvent.Unlock()

	m.changeEventHandler = append(m.changeEventHandler, handle)
}

func (m *ClientProvider) applyDeleteAddr(deleteAddr []string) {
	for _, handle := range m.getHandlers() {
		handle(deleteAddr)
	}
}

func (m *ClientProvider) getHandlers() []deleteAddrHandler {
	m.muChangeEvent.Lock()
	defer m.muChangeEvent.Unlock()

	return m.changeEventHandler
}

func (m *ClientProvider) compareAndApplyCopyData(servCopy servCopyCollect) {
	deleteAddr := make([]string, 0)
	m.muServlist.Lock()
	defer m.muServlist.Unlock()
	for sid, data := range m.servCopy {
		if data == nil || data.reg == nil || data.reg.Servs == nil {
			continue
		}
		for procName, info := range data.reg.Servs {
			// sid在新的servCopy中不存在，认为被删掉，加入info的addr
			if servCopy[sid] == nil {
				deleteAddr = append(deleteAddr, info.Addr)
				continue
			}
			if servCopy[sid].reg == nil {
				deleteAddr = append(deleteAddr, info.Addr)
				continue
			}
			if servCopy[sid].reg.Servs == nil {
				deleteAddr = append(deleteAddr, info.Addr)
				continue
			}
			// 地址被修改
			if servCopy[sid].reg.Servs[procName].Addr != info.Addr {
				deleteAddr = append(deleteAddr, info.Addr)
			}
		}
	}
	go m.applyDeleteAddr(deleteAddr)
}
