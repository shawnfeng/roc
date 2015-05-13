package rocserv

import (
	"fmt"
	"time"
	"strconv"
	"sort"
	"sync"
	"encoding/json"
	"hash/crc32"

    "github.com/coreos/go-etcd/etcd"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
)


type ClientEtcdV2 struct {
	etcdAddrs []string
	servLocation string
	servName string

	etcdClient *etcd.Client

	servPath string

	// 缓存地址列表，按照service id 降序的顺序存储
	// 按照processor 进行分组

	muServlist sync.Mutex
	servList map[string][]*ServInfo

}


func NewClientEtcdV2(etcdaddrs[]string, servlocation, servname string) (*ClientEtcdV2, error) {

    client := etcd.NewClient(etcdaddrs)
	if client == nil {
		return nil, fmt.Errorf("create etchd client error")
	}


	cli := &ClientEtcdV2 {
		etcdAddrs: etcdaddrs,
		servLocation: servlocation,
		servName: servname,

		servPath: fmt.Sprintf("%s/%s/%s",servlocation, BASE_ROUTE_KEY, servname),

		etcdClient: client,


	}

	cli.watch()

	return cli, nil

}

func (m *ClientEtcdV2) startWatch(chg chan *etcd.Response) {
	fun := "ClientEtcdV2.startWatch -->"

	path := m.servPath
	// 先get获取value，watch不能获取到现有的

    r, err := m.etcdClient.Get(path, false, true)
	if err != nil {
		slog.Errorf("%s get err:%s", fun, err)
		close(chg)
		return
	} else {
		chg <- r
	}

	_, err = m.etcdClient.Watch(path, 0, true, chg, nil)
	// etcd 关闭时候会返回
	if err != nil {
		slog.Errorf("%s watch err:%s", fun, err)

	}

}

func (m *ClientEtcdV2) watch() {
	fun := "ClientEtcdV2.watch -->"

	backoff := stime.NewBackOffCtrl(time.Millisecond * 10, time.Second * 5)

	var chg chan *etcd.Response

	go func() {
		slog.Infof("%s start watch", fun)
		for {
			slog.Infof("%s loop watch", fun)
			if chg == nil {
				slog.Infof("%s loop watch new receiver", fun)
				chg = make(chan *etcd.Response)
				go m.startWatch(chg)
			}

			r, ok := <-chg
			if !ok {
				slog.Errorf("%s chg info nil", fun)
				chg = nil
				backoff.BackOff()
			} else {
				slog.Infof("%s update v:%s", fun, r.Node.Key)
				m.parseResponse()
				backoff.Reset()
			}

		}
	}()
}


func (m *ClientEtcdV2) parseResponse() {
	fun := "ClientEtcdV2.parseResponse -->"

    r, err := m.etcdClient.Get(m.servPath, false, true)
	if err != nil {
		slog.Errorf("%s get err:%s", fun, err)
	}

	if r == nil {
		slog.Errorf("%s nil", fun)
		return
	}

	slog.Infof("%s chg action:%s nodes:%d etcdindex:%d raftindex:%d raftterm:%d", fun, r.Action, len(r.Node.Nodes), r.EtcdIndex, r.RaftIndex, r.RaftTerm )

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

	vs := make([]string, 0)
	for _, i := range ids {
		vs = append(vs, idServ[i])
	}


	servList := make(map[string][]*ServInfo)
	for _, s := range vs {
		var servs map[string]*ServInfo
		err = json.Unmarshal([]byte(s), &servs)
		if err != nil {
			slog.Errorf("%s json:%s error:%s", fun, s, err)
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

	slog.Infof("%s update:%s", fun, m.servList)
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



