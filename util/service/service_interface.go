// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"

	etcd "github.com/coreos/etcd/client"
)

type ServInfo struct {
	Type   string `json:"type"`
	Addr   string `json:"addr"`
	Servid int    `json:"-"`
	//Processor string    `json:"processor"`
}

func (m *ServInfo) String() string {
	return fmt.Sprintf("%s://%s", m.Type, m.Addr)
}

type RegData struct {
	Servs map[string]*ServInfo `json:"servs"`
	Lane  *string              `json:"lane"`
}

type ServCtrl struct {
	Weight  int      `json:"weight"`
	Disable bool     `json:"disable"`
	Groups  []string `json:"groups"`
}

type ManualData struct {
	Ctrl *ServCtrl `json:"ctrl"`
}

func NewRegData(servs map[string]*ServInfo, lane string) *RegData {
	return &RegData{
		Servs: servs,
		Lane:  &lane,
	}
}

func (r *RegData) GetLane() (string, bool) {
	if r.Lane == nil {
		return "", false
	}
	return *r.Lane, true
}

func getValue(client etcd.KeysAPI, path string) ([]byte, error) {
	r, err := client.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err != nil {
		return nil, err
	}

	if r.Node == nil || r.Node.Dir {
		return nil, fmt.Errorf("etcd node value err location:%s", path)
	}

	return []byte(r.Node.Value), nil
}

// ServBase Interface
type ServBase interface {
	// key is processor to ServInfo
	RegisterService(servs map[string]*ServInfo) error
	RegisterBackDoor(servs map[string]*ServInfo) error
	RegisterCrossDCService(servs map[string]*ServInfo) error

	Servname() string
	ServIp() string
	Servid() int
	// 服务副本名称, servename + servid
	Copyname() string

	// 获取服务的配置
	ServConfig(cfg interface{}) error
	// 任意路径的配置信息
	//ArbiConfig(location string) (string, error)

	// 默认的锁，局部分布式锁，各个服务之间独立不共享

	// 获取到lock立即返回，否则block直到获取到
	Lock(name string) error
	// 没有lock的情况下unlock，程序会直接panic
	Unlock(name string) error
	// 立即返回，如果获取到lock返回true，否则返回false
	Trylock(name string) (bool, error)

	// 全局分布式锁，全局只有一个，需要特殊加global说明

	LockGlobal(name string) error
	UnlockGlobal(name string) error
	TrylockGlobal(name string) (bool, error)

	// conf center
	ConfigCenter() xconfig.ConfigCenter

	// reginfos
	RegInfos() map[string]string

	// stop
	Stop()

	// set app shutdown hook
	SetOnShutdown(func())

	SetStartType(string)

	// wrap context with service context info, such as lane
	WithControlLaneInfo(ctx context.Context) context.Context

	InitReportLog(reporter Reporter)
}
