// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"fmt"
	"time"
	"strconv"
	"sort"
	"sync"
	"encoding/json"
	"hash/crc32"

    etcd "github.com/coreos/etcd/client"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"

	"golang.org/x/net/context"
)


type ClientEtcdV2 struct {
	confEtcd configEtcd
	servKey string
	servPath string

	etcdClient etcd.KeysAPI

	// 缓存地址列表，按照service id 降序的顺序存储
	// 按照processor 进行分组

	muServlist sync.Mutex
	servList map[string][]*ServInfo

}


func NewClientEtcdV2(confEtcd configEtcd, servlocation string) (*ClientEtcdV2, error) {

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


	cli := &ClientEtcdV2 {
		confEtcd: confEtcd,
		servKey: servlocation,
		servPath: fmt.Sprintf("%s/%s/%s", confEtcd.useBaseloc, BASE_LOC_DIST, servlocation),

		etcdClient: client,
	}

	cli.watch()

	return cli, nil

}

func (m *ClientEtcdV2) startWatch(chg chan *etcd.Response) {
	fun := "ClientEtcdV2.startWatch -->"

	path := m.servPath
	// 先get获取value，watch不能获取到现有的

    r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err != nil {
		slog.Errorf("%s get err:%s", fun, err)
		close(chg)
		return
	} else {
		chg <- r
	}

	slog.Infof("%s init get action:%s nodes:%d index:%d servPath:%s", fun, r.Action, len(r.Node.Nodes), r.Index, path)


	// !!! 这地方可能会丢掉变更， 后面需要调整
	wop := &etcd.WatcherOptions{
		Recursive: true,
	}
	watcher := m.etcdClient.Watcher(path, wop)
	if watcher == nil {
		slog.Errorf("%s new watcher", fun)
		return
	}

	for i := 0; ; i++ {
		resp, err := watcher.Next(context.Background())
		// etcd 关闭时候会返回
		if err != nil {
			slog.Errorf("%s watch err:%s", fun, err)
			close(chg)
			return
		} else {
			slog.Infof("%s next get idx:%d action:%s nodes:%d index:%d servPath:%s", fun, i, resp.Action, len(resp.Node.Nodes), resp.Index, path)
			chg <- resp
		}
	}

}

func (m *ClientEtcdV2) watch() {
	fun := "ClientEtcdV2.watch -->"

	backoff := stime.NewBackOffCtrl(time.Millisecond * 10, time.Second * 5)

	var chg chan *etcd.Response

	go func() {
		slog.Infof("%s start watch:%s", fun, m.servPath)
		for {
			//slog.Infof("%s loop watch", fun)
			if chg == nil {
				slog.Infof("%s loop watch new receiver:%s", fun, m.servPath)
				chg = make(chan *etcd.Response)
				go m.startWatch(chg)
			}

			r, ok := <-chg
			if !ok {
				slog.Errorf("%s chg info nil:%s", fun, m.servPath)
				chg = nil
				backoff.BackOff()
			} else {
				slog.Infof("%s update v:%s serv:%s", fun, r.Node.Key, m.servPath)
				m.parseResponse()
				backoff.Reset()
			}

		}
	}()
}


func (m *ClientEtcdV2) parseResponse() {
	fun := "ClientEtcdV2.parseResponse -->"
    r, err := m.etcdClient.Get(context.Background(), m.servPath, &etcd.GetOptions{Recursive: true, Sort: false})
	if err != nil {
		slog.Errorf("%s get err:%s", fun, err)
	}

	if r == nil {
		slog.Errorf("%s nil", fun)
		return
	}

	if !r.Node.Dir {
		slog.Errorf("%s not dir %s", fun, r.Node.Key)
		return
	}

	idServ := make(map[int]string)
	ids := make([]int, 0)
	for _, n := range r.Node.Nodes {
		sid := n.Key[len(r.Node.Key)+1:]
		id, err := strconv.Atoi(sid)
		if err != nil || id < 0 {
			slog.Errorf("%s sid error key:%s", fun, n.Key)
		} else {
			ids = append(ids, id)
			idServ[id] = n.Value
		}
	}
	sort.Ints(ids)

	slog.Infof("%s chg action:%s nodes:%d index:%d servPath:%s len:%d", fun, r.Action, len(r.Node.Nodes), r.Index, m.servPath, len(ids))
	if len(ids) == 0 {
		slog.Errorf("%s not found service path:%s please check deploy", fun, m.servPath)
	}

	//slog.Infof("%s chg servpath:%s ids:%v", fun, r.Action, len(r.Node.Nodes), r.EtcdIndex, r.RaftIndex, r.RaftTerm, m.servPath, ids)

	vs := make([]string, 0)
	for _, i := range ids {
		vs = append(vs, idServ[i])
	}


	servList := make(map[string][]*ServInfo)
	for _, s := range vs {
		var servs map[string]*ServInfo
		err = json.Unmarshal([]byte(s), &servs)
		if err != nil {
			slog.Errorf("%s servpath:%s json:%s error:%s", fun, m.servPath, s, err)
		}

		if len(servs) == 0 {
			slog.Errorf("%s not found copy path:%s info:%s please check deploy", fun, m.servPath, s)
		}

		for k, v := range servs {
			_, ok := servList[k]
			if !ok {
				servList[k] = make([]*ServInfo, 0)
			}
			servList[k] = append(servList[k], v)
		}

	}

	m.upServlist(servList)
}

func (m *ClientEtcdV2) upServlist(slist map[string][]*ServInfo) {
	fun := "ClientEtcdV2.upServlist -->"
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	m.servList = slist

	slog.Infof("%s serv:%s update:%s", fun, m.servPath, m.servList)
}

func (m *ClientEtcdV2) GetServAddr(processor, key string) *ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	if p, ok := m.servList[processor]; ok {
		if len(p) > 0 {
			h := crc32.ChecksumIEEE([]byte(key))

			return p[h % uint32(len(p))]
		}

	}
	return nil
}


func (m *ClientEtcdV2) GetAllServAddr() map[string][]*ServInfo {
	m.muServlist.Lock()
	defer m.muServlist.Unlock()

	rv := make(map[string][]*ServInfo)
	for k, v := range m.servList {
		n := make([]*ServInfo, len(v))
		copy(n, v)
		rv[k] = n
	}


	return rv
}


func (m *ClientEtcdV2) ServKey() string {
	return m.servKey
}


func (m *ClientEtcdV2) ServPath() string {
	return m.servPath
}
