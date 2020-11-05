// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// 包含服务注册、服务发现相关的定义、实现
package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"

	etcd "github.com/coreos/etcd/client"
	"github.com/shawnfeng/consistent"
)

type servCopyStr struct {
	servId int
	reg    string
	manual string
}

type servCopyData struct {
	servId int
	reg    *RegData
	manual *ManualData
}

type servCopyCollect map[int]*servCopyData

type ClientEtcdV2 struct {
	confEtcd configEtcd
	servKey  string // 形如 {servGroup}/{servName}
	servPath string
	// 使用的注册器位置，不同版本会注册到不同版本的dist目录
	// 但是会保持多版本的兼容，客户端优先使用最新版本的
	distLoc string

	etcdClient etcd.KeysAPI

	// 缓存地址列表，按照service id 降序的顺序存储
	// 按照processor 进行分组

	muServlist sync.Mutex
	servCopy   servCopyCollect
	servHash   map[string]*consistent.Consistent
}

func checkDistVersion(client etcd.KeysAPI, prefloc, servlocation string) string {
	fun := "checkDistVersion -->"
	ctx := context.Background()

	path := fmt.Sprintf("%s/%s/%s", prefloc, BASE_LOC_DIST_V2, servlocation)

	r, err := client.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err == nil {
		xlog.Infof(ctx, "%s check dist v2 ok path:%s", fun, path)
		for _, n := range r.Node.Nodes {
			for _, nc := range n.Nodes {
				if nc.Key == n.Key+"/"+BASE_LOC_REG_SERV && len(nc.Value) > 0 {
					return BASE_LOC_DIST_V2
				}
			}
		}
	}

	xlog.Warnf(ctx, "%s check dist v2 path: %s err: %v", fun, path, err)

	path = fmt.Sprintf("%s/%s/%s", prefloc, BASE_LOC_DIST, servlocation)

	r, err = client.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err == nil {
		xlog.Infof(ctx, "%s check dist v1 ok path:%s", fun, path)
		if len(r.Node.Nodes) > 0 {
			return BASE_LOC_DIST
		}
	}

	xlog.Warnf(ctx, "%s use v2 if check dist v1 path: %s err: %v", fun, path, err)

	return BASE_LOC_DIST_V2
}

func NewClientEtcdV2(confEtcd configEtcd, servlocation string) (*ClientEtcdV2, error) {
	//fun := "NewClientEtcdV2 -->"

	cfg := etcd.Config{
		Endpoints: confEtcd.etcdAddrs,
		Transport: etcd.DefaultTransport,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create etchd client cfg error")
	}

	client := etcd.NewKeysAPI(c)
	if client == nil {
		return nil, fmt.Errorf("create etchd api error")
	}

	distloc := checkDistVersion(client, confEtcd.useBaseloc, servlocation)

	cli := &ClientEtcdV2{
		confEtcd: confEtcd,
		servKey:  servlocation,
		distLoc:  distloc,
		servPath: fmt.Sprintf("%s/%s/%s", confEtcd.useBaseloc, distloc, servlocation),

		etcdClient: client,
	}

	cli.watch(cli.servPath, cli.parseResponse, time.Second*5)
	return cli, nil
}

func (m *ClientEtcdV2) startWatch(chg chan *etcd.Response, path string) {
	fun := "ClientEtcdV2.startWatch -->"
	ctx := context.Background()
	for i := 0; ; i++ {
		r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
		if err != nil {
			// TODO 因为目前breaker都报错key not found，所以用info，这里继续保持info的方式，后续再优化吧
			xlog.Infof(ctx, "%s get path: %s err: %v", fun, path, err)
			close(chg)
			return

		} else {
			chg <- r
		}

		index := uint64(0)
		if r != nil {
			index = r.Index
		}

		// 每次循环都设置下，测试发现放外边不好使
		wop := &etcd.WatcherOptions{
			Recursive:  true,
			AfterIndex: index,
		}
		watcher := m.etcdClient.Watcher(path, wop)
		if watcher == nil {
			xlog.Errorf(ctx, "%s new watcher path:%s", fun, path)
			close(chg)
			return
		}

		resp, err := watcher.Next(context.Background())
		// etcd 关闭时候会返回
		if err != nil {
			xlog.Errorf(ctx, "%s watch path: %s err: %v", fun, path, err)
			close(chg)
			return
		} else {
			xlog.Infof(ctx, "%s next get idx: %d action: %s nodes: %d index: %d after: %d servPath: %s", fun, i, resp.Action, len(resp.Node.Nodes), resp.Index, wop.AfterIndex, path)
			// 测试发现next获取到的返回，index，重新获取总有问题，触发两次，不确定，为什么？为什么？
			// 所以这里每次next前使用的afterindex都重新get了
		}
	}

}

func (m *ClientEtcdV2) watch(path string, handler func(*etcd.Response), d time.Duration) {
	fun := "ClientEtcdV2.watch -->"
	ctx := context.Background()

	backoff := xtime.NewBackOffCtrl(time.Millisecond*100, d)

	firstSync := make(chan bool)
	var firstOnce sync.Once

	var chg chan *etcd.Response
	go func() {
		xlog.Infof(ctx, "%s start watch:%s", fun, path)
		for {
			if chg == nil {
				xlog.Infof(ctx, "%s loop watch new receiver:%s", fun, path)
				chg = make(chan *etcd.Response)
				go m.startWatch(chg, path)
			}

			r, ok := <-chg
			if !ok {
				chg = nil

				firstOnce.Do(func() {
					close(firstSync)
				})

				backoff.BackOff()
			} else {
				xlog.Infof(ctx, "%s update v:%s serv:%s", fun, r.Node.Key, path)
				handler(r)

				firstOnce.Do(func() {
					close(firstSync)
				})

				backoff.Reset()
			}
		}
	}()

	select {
	case <-firstSync:
		xlog.Infof(ctx, "%s init ok, serv:%s", fun, path)
		return
	case <-time.After(time.Second):
		xlog.Warnf(ctx, "%s init timeout, serv:%s", fun, path)
		return
	}
}

func (m *ClientEtcdV2) parseResponse(r *etcd.Response) {
	fun := "ClientEtcdV2.parseResponse -->"
	ctx := context.Background()
	if !r.Node.Dir {
		xlog.Errorf(ctx, "%s not dir %s", fun, r.Node.Key)
		return
	}

	if m.distLoc == BASE_LOC_DIST {
		m.parseResponseV1(r)
	} else if m.distLoc == BASE_LOC_DIST_V2 {
		m.parseResponseV2(r)
	} else {
		xlog.Errorf(ctx, "%s not support:%s dir:%s", fun, m.distLoc, r.Node.Key)
	}

}

func (m *ClientEtcdV2) parseResponseV2(r *etcd.Response) {
	fun := "ClientEtcdV2.parseResponseV2 -->"
	ctx := context.Background()

	idServ := make(map[int]*servCopyStr)
	ids := make([]int, 0)
	for _, n := range r.Node.Nodes {
		if !n.Dir {
			xlog.Errorf(context.Background(), "%s not dir %s", fun, n.Key)
			return
		}

		sid := n.Key[len(r.Node.Key)+1:]
		id, err := strconv.Atoi(sid)
		if err != nil || id < 0 {
			xlog.Errorf(context.Background(), "%s sid error key:%s", fun, n.Key)
			continue
		}
		ids = append(ids, id)

		var reg, manual string
		for _, nc := range n.Nodes {
			if nc.Key == n.Key+"/"+BASE_LOC_REG_SERV {
				reg = nc.Value
			} else if nc.Key == n.Key+"/"+BASE_LOC_REG_MANUAL {
				manual = nc.Value
			}
		}
		idServ[id] = &servCopyStr{
			servId: id,
			reg:    reg,
			manual: manual,
		}

	}
	sort.Ints(ids)

	xlog.Infof(ctx, "%s chg action:%s nodes:%d index:%d servPath:%s len:%d", fun, r.Action, len(r.Node.Nodes), r.Index, m.servPath, len(ids))
	if len(ids) == 0 {
		xlog.Errorf(ctx, "%s not found service path:%s please check deploy", fun, m.servPath)
	}

	servCopy := make(servCopyCollect)
	//for _, s := range vs {
	for _, i := range ids {
		is := idServ[i]
		if is == nil {
			xlog.Warnf(ctx, "%s serv not found idx:%d servpath:%s", fun, i, m.servPath)
			continue
		}

		var regd RegData
		if len(is.reg) > 0 {
			err := json.Unmarshal([]byte(is.reg), &regd)
			if err != nil {
				xlog.Warnf(ctx, "%s servpath: %s sid: %d json: %s error: %v", fun, m.servPath, i, is.reg, err)
			}
			if len(regd.Servs) == 0 {
				xlog.Warnf(ctx, "%s not found copy path: %s sid: %d info: %s please check deploy", fun, m.servPath, i, is.reg)
			}
		}

		var manual ManualData
		if len(is.manual) > 0 {
			err := json.Unmarshal([]byte(is.manual), &manual)
			if err != nil {
				xlog.Errorf(ctx, "%s servpath: %s json: %s err: %v", fun, m.servPath, is.manual, err)
			}
		}

		if manual.Ctrl == nil {
			manual.Ctrl = &ServCtrl{}
		}

		if len(manual.Ctrl.Groups) == 0 {
			manual.Ctrl.Groups = append(manual.Ctrl.Groups, "")
		}

		servCopy[i] = &servCopyData{
			servId: i,
			reg:    &regd,
			manual: &manual,
		}

	}

	m.upServlist(servCopy)
}

func (m *ClientEtcdV2) parseResponseV1(r *etcd.Response) {
	fun := "ClientEtcdV2.parseResponseV1 -->"
	ctx := context.Background()

	idServ := make(map[int]string)
	ids := make([]int, 0)
	for _, n := range r.Node.Nodes {
		sid := n.Key[len(r.Node.Key)+1:]
		id, err := strconv.Atoi(sid)
		if err != nil || id < 0 {
			xlog.Errorf(ctx, "%s sid error key:%s", fun, n.Key)
		} else {
			xlog.Infof(ctx, "%s dist key:%s value:%s", fun, n.Key, n.Value)
			ids = append(ids, id)
			idServ[id] = n.Value
		}
	}
	sort.Ints(ids)

	xlog.Infof(ctx, "%s chg action:%s nodes:%d index:%d servPath:%s len:%d", fun, r.Action, len(r.Node.Nodes), r.Index, m.servPath, len(ids))
	if len(ids) == 0 {
		xlog.Errorf(ctx, "%s not found service path:%s please check deploy", fun, m.servPath)
	}

	servCopy := make(servCopyCollect)
	//for _, s := range vs {
	for _, i := range ids {
		s := idServ[i]

		var servs map[string]*ServInfo
		err := json.Unmarshal([]byte(s), &servs)
		if err != nil {
			xlog.Errorf(ctx, "%s servpath: %s json: %s err: %v", fun, m.servPath, s, err)
		}

		if len(servs) == 0 {
			xlog.Errorf(ctx, "%s not found copy path:%s info:%s please check deploy", fun, m.servPath, s)
		}

		servCopy[i] = &servCopyData{
			servId: i,
			// WARNING: v1版本不支持新版泳道元信息获取
			reg: &RegData{
				Servs: servs,
			},
			manual: &ManualData{
				Ctrl: &ServCtrl{
					Weight:  0,
					Disable: false,
					Groups:  []string{""},
				},
			},
		}

	}

	m.upServlist(servCopy)
}

func (m *ClientEtcdV2) upServlist(scopy map[int]*servCopyData) {
	fun := "ClientEtcdV2.upServlist -->"
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

	shash := make(map[string]*consistent.Consistent)
	for group, list := range slist {
		hash := consistent.NewWithElts(list)
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

func (m *ClientEtcdV2) GetServAddr(processor, key string) *ServInfo {
	//fun := "ClientEtcdV2.GetServAddr -->"
	return m.GetServAddrWithGroup("", processor, key)
}

func (m *ClientEtcdV2) GetServAddrWithGroup(group string, processor, key string) *ServInfo {
	fun := "ClientEtcdV2.GetServAddrWithGroup-->"
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

func (m *ClientEtcdV2) getServAddrWithServid(servid int, processor, key string) *ServInfo {
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

func (m *ClientEtcdV2) GetServAddrWithServid(servid int, processor, key string) *ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	return m.getServAddrWithServid(servid, processor, key)
}

func (m *ClientEtcdV2) GetAllServAddr(processor string) []*ServInfo {
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

func (m *ClientEtcdV2) GetAllServAddrWithGroup(group, processor string) []*ServInfo {
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

func (m *ClientEtcdV2) ServKey() string {
	return m.servKey
}

func (m *ClientEtcdV2) ServPath() string {
	return m.servPath
}

func (m *ClientEtcdV2) String() string {
	return fmt.Sprintf("service_key: %s, service_path: %s", m.servKey, m.servPath)
}

// 兼容新旧版本的lane metadata
func (s *servCopyData) containsLane(lane string) bool {
	if s.reg != nil {
		l, ok := s.reg.GetLane()
		xlog.Debugf(context.Background(), "containsLane get v2 lane metadata, ok: %v, regInfo: %v, expect: %s, actual: %s", ok, s.reg.Servs, lane, l)
		if ok {
			if l == lane {
				return true
			}
		}
	}

	xlog.Debugf(context.Background(), "containsLane get v1 lane metadata, servId: %d, expect: %s", s.servId, lane)
	if s.manual == nil || s.manual.Ctrl == nil {
		return false
	}
	for _, l := range s.manual.Ctrl.Groups {
		if l == lane {
			return true
		}
	}
	return false
}
