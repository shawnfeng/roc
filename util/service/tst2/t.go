// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"

	etcd "github.com/coreos/etcd/client"

	"github.com/shawnfeng/sutil/slog"

	"golang.org/x/net/context"
)

func startWatch(etcdClient etcd.KeysAPI, path string) {
	fun := "startWatch -->"

	for i := 0; ; i++ {
		r, err := etcdClient.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
		if err != nil {
			slog.Errorf("%s get path:%s err:%s", fun, path, err)
			return
		}

		sresp, _ := json.Marshal(r)
		slog.Infof("%s init get action:%s nodes:%d index:%d servPath:%s resp:%s", fun, r.Action, len(r.Node.Nodes), r.Index, path, sresp)

		// 每次循环都设置下，测试发现放外边不好使
		wop := &etcd.WatcherOptions{
			Recursive:  true,
			AfterIndex: r.Index,
		}
		watcher := etcdClient.Watcher(path, wop)
		if watcher == nil {
			slog.Errorf("%s new watcher path:%s", fun, path)
			return
		}

		resp, err := watcher.Next(context.Background())
		// etcd 关闭时候会返回
		if err != nil {
			slog.Errorf("%s watch path:%s err:%s", fun, path, err)
			return
		} else {
			sresp, _ := json.Marshal(resp)
			slog.Infof("%s next get idx:%d action:%s nodes:%d index:%d after:%d servPath:%s resp:%s", fun, i, resp.Action, len(resp.Node.Nodes), resp.Index, wop.AfterIndex, path, sresp)
			// 测试发现next获取到的返回，index，重新获取总有问题，触发两次，不确定，为什么？为什么？
			// 所以这里每次next前使用的afterindex都重新get了
		}
	}

}

func main() {

	cfg := etcd.Config{
		Endpoints: []string{"http://127.0.0.1:20002"},
		Transport: etcd.DefaultTransport,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		slog.Errorln(cfg, err)
	}

	client := etcd.NewKeysAPI(c)
	if client == nil {
		slog.Errorln(cfg, err)
	}

	startWatch(client, "/roc/dist/rec/recommend")

}
