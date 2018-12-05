// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"encoding/json"
	"fmt"
	"time"
	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
	//"github.com/coreos/go-etcd/etcd"
	etcd "github.com/coreos/etcd/client"

	"github.com/shawnfeng/sutil/sconf"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/slowid"
	"github.com/shawnfeng/sutil/ssync"

	"github.com/shawnfeng/dbrouter"

	"golang.org/x/net/context"
)

const (
	BASE_LOC_DIST = "dist"
	// 调整了服务注册结构，为兼容老版本，BASE_LOC_DIST下也要完成之前方式的注册
	// dist2 2为版本2
	BASE_LOC_DIST_V2        = "dist2"
	BASE_LOC_BREAKER        = "breaker"
	BASE_LOC_BREAKER_GLOBAL = "breaker/global"
	BASE_LOC_ETC            = "etc"
	BASE_LOC_ETC_GLOBAL     = "etc/global"
	BASE_LOC_SKEY           = "skey"
	BASE_LOC_OP             = "op"
	BASE_LOC_DB             = "db/route"
	// 服务内分布式锁，只在单个服务副本之间使用
	BASE_LOC_LOCAL_DIST_LOCK = "lock/local"
	// 全局分布式锁，跨服务使用
	BASE_LOC_GLOBAL_DIST_LOCK = "lock/global"

	// 服务注册的位置
	BASE_LOC_REG_SERV = "serve"
	// 后门注册的位置
	BASE_LOC_REG_BACKDOOR = "backdoor"

	// 服务手动配置位置
	BASE_LOC_REG_MANUAL = "manual"
)

type configEtcd struct {
	etcdAddrs  []string
	useBaseloc string
}

type ServBaseV2 struct {
	IdGenerator

	confEtcd configEtcd

	dbLocation   string
	servLocation string
	copyName     string
	sessKey      string

	etcdClient etcd.KeysAPI
	servId     int

	dbRouter *dbrouter.Router

	muLocks ssync.Mutex
	locks   map[string]*ssync.Mutex

	muHearts ssync.Mutex
	hearts   map[string]*distLockHeart
}

func (m *ServBaseV2) RegisterBackDoor(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterBackDoor -->"
	rd := &RegData{
		Servs: servs,
	}

	js, err := json.Marshal(rd)
	if err != nil {
		return err
	}

	slog.Infof("%s servs:%s", fun, js)

	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, BASE_LOC_REG_BACKDOOR)

	return m.doRegister(path, string(js), true)

}

// {type:http/thrift, addr:10.3.3.3:23233, processor:fuck}
func (m *ServBaseV2) RegisterService(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterService -->"
	err := m.RegisterServiceV2(servs)
	if err != nil {
		slog.Errorf("%s reg v2 err:%s", fun, err)
		return err
	}

	err = m.RegisterServiceV1(servs)
	if err != nil {
		slog.Errorf("%s reg v1 err:%s", fun, err)
		return err
	}

	slog.Infof("%s regist ok", fun)

	return nil
}

func (m *ServBaseV2) RegisterServiceV2(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterServiceV2 -->"

	rd := &RegData{
		Servs: servs,
	}

	js, err := json.Marshal(rd)
	if err != nil {
		return err
	}

	slog.Infof("%s servs:%s", fun, js)

	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, BASE_LOC_REG_SERV)

	return m.doRegister(path, string(js), true)
}

// 为兼容老的client发现服务，保留的
func (m *ServBaseV2) RegisterServiceV1(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterServiceV1 -->"

	js, err := json.Marshal(servs)
	if err != nil {
		return err
	}

	slog.Infof("%s servs:%s", fun, js)

	path := fmt.Sprintf("%s/%s/%s/%d", m.confEtcd.useBaseloc, BASE_LOC_DIST, m.servLocation, m.servId)

	return m.doRegister(path, string(js), true)
}

func (m *ServBaseV2) doRegister(path, js string, refresh bool) error {
	fun := "ServBaseV2.doRegister -->"
	// 创建完成标志
	var iscreate bool

	go func() {

		for i := 0; ; i++ {
			var err error
			var r *etcd.Response
			if !iscreate {
				slog.Warnf("%s create idx:%d servs:%s", fun, i, js)
				r, err = m.etcdClient.Set(context.Background(), path, js, &etcd.SetOptions{
					TTL: time.Second * 180,
				})
			} else {
				if refresh {
					// 在刷新ttl时候，不允许变更value
					// 节点超时时间为120秒
					slog.Infof("%s refresh ttl idx:%d servs:%s", fun, i, js)
					r, err = m.etcdClient.Set(context.Background(), path, "", &etcd.SetOptions{
						PrevExist: etcd.PrevExist,
						TTL:       time.Second * 180,
						Refresh:   true,
					})
				} else {
					r, err = m.etcdClient.Set(context.Background(), path, js, &etcd.SetOptions{
						TTL: time.Second * 180,
					})
				}

			}

			if err != nil {
				iscreate = false
				slog.Errorf("%s reg idx:%d err:%s", fun, i, err)

			} else {
				iscreate = true
				jr, _ := json.Marshal(r)
				slog.Infof("%s reg idx:%d ok:%s", fun, i, jr)
			}

			// 每分发起一次注册
			time.Sleep(time.Second * 30)
		}

	}()

	return nil
}

func (m *ServBaseV2) Servid() int {
	return m.servId
}

func (m *ServBaseV2) Copyname() string {
	return fmt.Sprintf("%s%d", m.servLocation, m.servId)

}

func (m *ServBaseV2) Servname() string {
	return m.servLocation
}

func (m *ServBaseV2) Dbrouter() *dbrouter.Router {
	return m.dbRouter
}

func (m *ServBaseV2) ServConfig(cfg interface{}) error {
	fun := "ServBaseV2.ServConfig -->"
	// 获取全局配置
	path := fmt.Sprintf("%s/%s", m.confEtcd.useBaseloc, BASE_LOC_ETC_GLOBAL)
	scfg_global, err := getValue(m.etcdClient, path)
	if err != nil {
		slog.Warnf("%s serv config global value path:%s err:%s", fun, path, err)
	}
	slog.Infof("%s global cfg:%s path:%s", fun, scfg_global, path)

	path = fmt.Sprintf("%s/%s/%s", m.confEtcd.useBaseloc, BASE_LOC_ETC, m.servLocation)
	scfg, err := getValue(m.etcdClient, path)
	if err != nil {
		slog.Warnf("%s serv config value path:%s err:%s", fun, path, err)
	}

	slog.Infof("%s cfg:%s path:%s", fun, scfg, path)
	tf := sconf.NewTierConf()
	err = tf.Load(scfg_global)
	if err != nil {
		return err
	}

	err = tf.Load(scfg)
	if err != nil {
		return err
	}

	err = tf.Unmarshal(cfg)
	if err != nil {
		return err
	}

	return nil
}

// etcd v2 接口
func NewServBaseV2(confEtcd configEtcd, servLocation, skey string) (*ServBaseV2, error) {
	fun := "NewServBaseV2 -->"

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

	path := fmt.Sprintf("%s/%s/%s", confEtcd.useBaseloc, BASE_LOC_SKEY, servLocation)

	sid, err := retryGenSid(client, path, skey, 3)
	if err != nil {
		return nil, err
	}

	slog.Infof("%s path:%s sid:%d skey:%s", fun, path, sid, skey)

	dbloc := fmt.Sprintf("%s/%s", confEtcd.useBaseloc, BASE_LOC_DB)

	var dr *dbrouter.Router
	jscfg, err := getValue(client, dbloc)
	if err != nil {
		slog.Warnf("%s db:%s config notfound", fun, dbloc)
	} else {
		dr, err = dbrouter.NewRouter(jscfg)
		if err != nil {
			return nil, err
		}
	}

	reg := &ServBaseV2{
		confEtcd:     confEtcd,
		dbLocation:   dbloc,
		servLocation: servLocation,
		sessKey:      skey,
		etcdClient:   client,
		servId:       sid,
		locks:        make(map[string]*ssync.Mutex),
		hearts:       make(map[string]*distLockHeart),

		dbRouter: dr,
	}

	sf, err := initSnowflake(sid)
	if err != nil {
		return nil, err
	}

	reg.IdGenerator.snow = sf
	reg.IdGenerator.slow = make(map[string]*slowid.Slowid)
	reg.IdGenerator.servId = sid

	return reg, nil

}

// mutex
