package rocserv

import (
	"fmt"
	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
    "github.com/coreos/go-etcd/etcd"

	"github.com/shawnfeng/sutil/slog"
)


type ServBaseV2 struct {
	IdGenerator

	etcdAddrs []string
	servLocation string
	servName string
	sessKey string

	etcdClient *etcd.Client
	servId int


}

// {type:http/thrift, addr:10.3.3.3:23233, processor:fuck}
func (m *ServBaseV2) UpdateService(svAddr string) {

}

func (m *ServBaseV2) Servid() int {
	return m.servId
}


// etcd v2 接口
func NewServBaseV2(etcdaddrs[]string, location, name, skey string) (*ServBaseV2, error) {
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


	reg := &ServBaseV2 {
		etcdAddrs: etcdaddrs,
		servLocation: location,
		servName: name,
		sessKey: skey,
		etcdClient: client,
		servId: sid,

	}


	sf, err := initSnowflake(sid)
	if err != nil {
		return nil, err
	}

	reg.IdGenerator.snow = sf

	return reg, nil

}


// mutex



