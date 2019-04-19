package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	etcd "github.com/coreos/etcd/client"
	"github.com/shawnfeng/consistent"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"sync"
	"time"
)

type GrpcClientEtcd struct {
	confEtcd configEtcd
	servKey  string
	servPath string
	// 使用的注册器位置，不同版本会注册到不同版本的dist目录
	// 但是会保持多版本的兼容，客户端优先使用最新版本的
	distLoc string

	etcdClient etcd.KeysAPI

	// 缓存地址列表，按照service id 降序的顺序存储
	// 按照processor 进行分组

	muServlist sync.Mutex
	servCopy   servCopyCollect
	servHash   *consistent.Consistent

	breakerGlobalPath string
	breakerServPath   string

	breakerMutex      sync.RWMutex
	breakerGlobalConf string
	breakerServConf   string
}

func NewGrpcClientEtcd(confEtcd configEtcd, servlocation string) (*GrpcClientEtcd, error) {
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

	cli := &GrpcClientEtcd{
		confEtcd: confEtcd,
		servKey:  servlocation,
		distLoc:  distloc,
		servPath: fmt.Sprintf("%s/%s/%s", confEtcd.useBaseloc, distloc, servlocation),

		etcdClient: client,

		breakerServPath:   fmt.Sprintf("%s/%s/%s", confEtcd.useBaseloc, BASE_LOC_BREAKER, servlocation),
		breakerGlobalPath: fmt.Sprintf("%s/%s", confEtcd.useBaseloc, BASE_LOC_BREAKER_GLOBAL),
	}

	cli.watch(cli.servPath, cli.parseResponse)
	cli.watch(cli.breakerServPath, cli.handleBreakerServResponse)
	cli.watch(cli.breakerGlobalPath, cli.handleBreakerGlobalResponse)

	return cli, nil

}

func (m *GrpcClientEtcd) startWatch(chg chan *etcd.Response, path string) {
	fun := "ClientEtcdV2.startWatch -->"

	for i := 0; ; i++ {
		r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
		if err != nil {
			slog.Warnf("%s get path:%s err:%s", fun, path, err)
			//close(chg)
			//return

		} else {
			chg <- r
		}

		index := uint64(0)
		if r != nil {
			index = r.Index
			sresp, _ := json.Marshal(r)
			slog.Infof("%s init get action:%s nodes:%d index:%d servPath:%s resp:%s", fun, r.Action, len(r.Node.Nodes), r.Index, path, sresp)
		}

		// 每次循环都设置下，测试发现放外边不好使
		wop := &etcd.WatcherOptions{
			Recursive:  true,
			AfterIndex: index,
		}
		watcher := m.etcdClient.Watcher(path, wop)
		if watcher == nil {
			slog.Errorf("%s new watcher path:%s", fun, path)
			return
		}

		resp, err := watcher.Next(context.Background())
		// etcd 关闭时候会返回
		if err != nil {
			slog.Errorf("%s watch path:%s err:%s", fun, path, err)
			close(chg)
			return
		} else {
			slog.Infof("%s next get idx:%d action:%s nodes:%d index:%d after:%d servPath:%s", fun, i, resp.Action, len(resp.Node.Nodes), resp.Index, wop.AfterIndex, path)
			// 测试发现next获取到的返回，index，重新获取总有问题，触发两次，不确定，为什么？为什么？
			// 所以这里每次next前使用的afterindex都重新get了
		}
	}

}

func (m *GrpcClientEtcd) watch(path string, hander func(*etcd.Response)) {
	fun := "ClientEtcdV2.watch -->"

	backoff := stime.NewBackOffCtrl(time.Millisecond*10, time.Second*5)

	var chg chan *etcd.Response

	go func() {
		slog.Infof("%s start watch:%s", fun, path)
		for {
			if chg == nil {
				slog.Infof("%s loop watch new receiver:%s", fun, path)
				chg = make(chan *etcd.Response)
				go m.startWatch(chg, path)
			}

			r, ok := <-chg
			if !ok {
				slog.Errorf("%s chg info nil:%s", fun, path)
				chg = nil
				backoff.BackOff()
			} else {
				slog.Infof("%s update v:%s serv:%s", fun, r.Node.Key, path)
				hander(r)
				backoff.Reset()
			}

		}
	}()
}

func (m *GrpcClientEtcd) parseResponse(r *etcd.Response) {
	fun := "ClientEtcdV2.parseResponse -->"
	if !r.Node.Dir {
		slog.Errorf("%s not dir %s", fun, r.Node.Key)
		return
	}

}

func (m *GrpcClientEtcd) handleBreakerGlobalResponse(r *etcd.Response) {
	fun := "ClientEtcdV2.handleBreakerGlobalResponse -->"

	if r.Node.Dir {
		slog.Errorf("%s not file %s", fun, r.Node.Key)
		return
	}

	m.breakerMutex.Lock()
	defer m.breakerMutex.Unlock()
	m.breakerGlobalConf = r.Node.Value
}

func (m *GrpcClientEtcd) handleBreakerServResponse(r *etcd.Response) {
	fun := "GrpcClientEtcd.handleBreakerServResponse -->"

	if r.Node.Dir {
		slog.Errorf("%s not file %s", fun, r.Node.Key)
		return
	}

	m.breakerMutex.Lock()
	defer m.breakerMutex.Unlock()
	m.breakerServConf = r.Node.Value
}
