// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"fmt"
	"time"
	"encoding/json"
	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
    //"github.com/coreos/go-etcd/etcd"
    etcd "github.com/coreos/etcd/client"

	"github.com/shawnfeng/sutil/sconf"
	"github.com/shawnfeng/sutil/slog"

	"github.com/shawnfeng/dbrouter"

	"golang.org/x/net/context"
)

const (
	BASE_LOC_DIST = "dist"
	BASE_LOC_ETC = "etc"
	BASE_LOC_ETC_GLOBAL = "etc/global"
	BASE_LOC_SKEY = "skey"
	BASE_LOC_OP = "op"
	BASE_LOC_DB = "db/route"
)


type configEtcd struct {
	etcdAddrs []string
	useBaseloc string
}


type ServBaseV2 struct {
	IdGenerator

	confEtcd configEtcd

	dbLocation string
	servLocation string
	copyName string
	sessKey string

	etcdClient etcd.KeysAPI
	servId int

	dbRouter *dbrouter.Router

}

// {type:http/thrift, addr:10.3.3.3:23233, processor:fuck}
func (m *ServBaseV2) RegisterService(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterService -->"


	js, err := json.Marshal(servs)
	if err != nil {
		return err
	}

	slog.Infof("%s servs:%s", fun, js)

	path := fmt.Sprintf("%s/%s/%s/%d", m.confEtcd.useBaseloc, BASE_LOC_DIST, m.servLocation, m.servId)
	// 创建完成标志
	var iscreate bool

	go func() {

		for i := 0; ; i++ {
			var r *etcd.Response
			if !iscreate {
				slog.Warnf("%s create idx:%d servs:%s", fun, i, js)
				r, err = m.etcdClient.Set(context.Background(), path, string(js), &etcd.SetOptions {
					TTL: time.Second*120,
				})
			} else {
				// 在刷新ttl时候，不允许变更value
				// 节点超时时间为120秒
				slog.Infof("%s refresh ttl idx:%d servs:%s", fun, i, js)
				r, err = m.etcdClient.Set(context.Background(), path, "", &etcd.SetOptions {
					PrevExist: etcd.PrevExist,
					TTL: time.Second*120,
					Refresh: true,
				})

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
			time.Sleep(time.Second * 60)
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



	reg := &ServBaseV2 {
		confEtcd: confEtcd,
		dbLocation: dbloc,
		servLocation: servLocation,
		sessKey: skey,
		etcdClient: client,
		servId: sid,

		dbRouter: dr,

	}


	sf, err := initSnowflake(sid)
	if err != nil {
		return nil, err
	}

	reg.IdGenerator.snow = sf

	return reg, nil

}


// mutex



