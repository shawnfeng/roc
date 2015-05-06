package rocserv

import (
	"fmt"
	"encoding/json"
	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
    "github.com/coreos/go-etcd/etcd"

	"github.com/shawnfeng/sutil/sconf"
	"github.com/shawnfeng/sutil/slog"

	"github.com/shawnfeng/roc/util/dbrouter"
)


type ServBaseV2 struct {
	IdGenerator

	etcdAddrs []string
	servLocation string
	etcLocation string
	dbLocation string
	servName string
	copyName string
	sessKey string

	etcdClient *etcd.Client
	servId int

	dbRouter *dbrouter.Router

}

// {type:http/thrift, addr:10.3.3.3:23233, processor:fuck}
func (m *ServBaseV2) UpdateService(servs map[string]*ServInfo) {

}

func (m *ServBaseV2) Servid() int {
	return m.servId
}


func (m *ServBaseV2) Copyname() string {
	return m.copyName

}

func (m *ServBaseV2) Dbrouter() *dbrouter.Router {
	return m.dbRouter
}

func (m *ServBaseV2) ServConfig(cfg interface{}) error {
	fun := "ServBaseV2.ServConfig"
	scfg, err := getValue(m.etcdClient, fmt.Sprintf("%s/%s", m.etcLocation, m.servName))
	if err != nil {
		slog.Warnf("%s --> serv config value err:%s", fun, err)
	}

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
func NewServBaseV2(etcdaddrs[]string, location, etcloc, dbloc, name, skey string) (*ServBaseV2, error) {
	fun := "NewServBaseV2"

    client := etcd.NewClient(etcdaddrs)
	if client == nil {
		return nil, fmt.Errorf("create etchd client error")
	}

	path := fmt.Sprintf("%s/%s/%s", location, BASE_SESSION_KEY, name)

	sid, err := retryGenSid(client, path, skey, 3)
	if err != nil {
		return nil, err
	}

	slog.Infof("%s --> path:%s sid:%d skey:%s", fun, path, sid, skey)


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
		servLocation: location,
		etcLocation: etcloc,
		dbLocation: dbloc,
		servName: name,
		copyName: fmt.Sprintf("%s%d", name, sid),
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



