// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
	"fmt"
	"time"
	"encoding/json"
	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
    "github.com/coreos/go-etcd/etcd"

	"github.com/shawnfeng/sutil/sconf"
	"github.com/shawnfeng/sutil/slog"

	"github.com/shawnfeng/roc/util/dbrouter"
)

const (
	BASE_LOC_DIST = "roc/dist"
	BASE_LOC_ETC = "roc/etc"
	BASE_LOC_SKEY = "roc/skey"
	BASE_LOC_OP = "roc/op"
	BASE_LOC_DB = "roc/db/route"
)


type ServBaseV2 struct {
	IdGenerator

	etcdAddrs []string
	etcLocation string
	dbLocation string
	servLocation string
	copyName string
	sessKey string

	etcdClient *etcd.Client
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

	path := fmt.Sprintf("/%s/%s/%d", BASE_LOC_DIST, m.servLocation, m.servId)


	go func() {

		for {
			// 节点超时时间为120秒
			r, err := m.etcdClient.Set(path, string(js), 120)
			if err != nil {
				slog.Errorf("%s reg err:%s", fun, err)
			} else {
				jr, _ := json.Marshal(r)
				slog.Infof("%s reg ok:%s", fun, jr)
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

func (m *ServBaseV2) Dbrouter() *dbrouter.Router {
	return m.dbRouter
}

func (m *ServBaseV2) ServConfig(cfg interface{}) error {
	fun := "ServBaseV2.ServConfig -->"
	scfg, err := getValue(m.etcdClient, fmt.Sprintf("/%s/%s", BASE_LOC_ETC, m.servLocation))
	if err != nil {
		slog.Warnf("%s serv config value err:%s", fun, err)
	}
	slog.Infof("%s cfg:%s %s %s", fun, scfg, BASE_LOC_ETC, m.servLocation)
	tf := sconf.NewTierConf()
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
func NewServBaseV2(etcdaddrs[]string, servLocation, skey string) (*ServBaseV2, error) {
	fun := "NewServBaseV2 -->"

    client := etcd.NewClient(etcdaddrs)
	if client == nil {
		return nil, fmt.Errorf("create etchd client error")
	}

	path := fmt.Sprintf("/%s/%s", BASE_LOC_SKEY, servLocation)

	sid, err := retryGenSid(client, path, skey, 3)
	if err != nil {
		return nil, err
	}

	slog.Infof("%s path:%s sid:%d skey:%s", fun, path, sid, skey)


	dbloc := BASE_LOC_DB
	var dr *dbrouter.Router
	if len(dbloc) > 0 {
		jscfg, err := getValue(client, dbloc)
		if err != nil {
			return nil, err
		}

		var dbcfg dbrouter.Config
		err = json.Unmarshal(jscfg, &dbcfg)
		if err != nil {
			return nil, err
		}


		dr, err = dbrouter.NewRouter(&dbcfg)
		if err != nil {
			return nil, err
		}
	}


	reg := &ServBaseV2 {
		etcdAddrs: etcdaddrs,
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



