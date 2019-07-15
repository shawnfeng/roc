// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	// now use 73a8ef737e8ea002281a28b4cb92a1de121ad4c6
	//"github.com/coreos/go-etcd/etcd"

	etcd "github.com/coreos/etcd/client"

	"github.com/sdming/gosnow"

	"github.com/shawnfeng/sutil"
	"github.com/shawnfeng/sutil/dbrouter"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/slowid"

	"golang.org/x/net/context"
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
}

func (m *RegData) String() string {
	var procs []string
	for k, v := range m.Servs {
		procs = append(procs, fmt.Sprintf("%s@%s", v, k))
	}
	return strings.Join(procs, "|")
}

type ServCtrl struct {
	Weight  int  `json:"weight"`
	Disable bool `json:"disable"`
}

type ManualData struct {
	Ctrl *ServCtrl `json:"ctrl"`
}

func (m *ManualData) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}

// ServBase Interface
type ServBase interface {
	// key is processor to ServInfo
	RegisterService(servs map[string]*ServInfo) error
	RegisterBackDoor(servs map[string]*ServInfo) error

	Servname() string
	Servid() int
	// 服务副本名称, servename + servid
	Copyname() string

	// 获取服务的配置
	ServConfig(cfg interface{}) error
	// 任意路径的配置信息
	//ArbiConfig(location string) (string, error)

	// 慢id生成器，适合id产生不是非常快的场景,基于毫秒时间戳，每毫秒最多产生2个id，过快会自动阻塞，直到毫秒递增
	// id表示可以再52bit完成，用double表示不会丢失精度，javascript等弱类型语音可以直接使用
	GenSlowId(tp string) (int64, error)
	GetSlowIdStamp(sid int64) int64
	GetSlowIdWithStamp(stamp int64) int64

	// id生成逻辑
	GenSnowFlakeId() (int64, error)
	// 获取snowflakeid生成时间戳，单位ms
	GetSnowFlakeIdStamp(sid int64) int64
	// 按给定的时间点构造一个起始snowflakeid，一般用于区域判断
	GetSnowFlakeIdWithStamp(stamp int64) int64

	GenUuid() (string, error)
	GenUuidSha1() (string, error)
	GenUuidMd5() (string, error)

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

	// db router
	Dbrouter() *dbrouter.Router
}

//====================
// id生成逻辑
type IdGenerator struct {
	servId int
	mu     sync.Mutex
	slow   map[string]*slowid.Slowid
	snow   *gosnow.SnowFlake
}

func (m *IdGenerator) GenSlowId(tp string) (int64, error) {
	gslow := func(tp string) (*slowid.Slowid, error) {
		m.mu.Lock()
		defer m.mu.Unlock()

		s := m.slow[tp]
		if s == nil {
			sl, err := initSlowid(m.servId)
			if err != nil {
				return nil, err
			}
			m.slow[tp] = sl
			s = sl
		}
		return s, nil
	}

	s, err := gslow(tp)
	if err != nil {
		return 0, err
	}

	return s.Next()
}

func (m *IdGenerator) GetSlowIdStamp(sid int64) int64 {
	return slowid.Since + sid>>11
}

func (m *IdGenerator) GetSlowIdWithStamp(stamp int64) int64 {
	return (stamp - slowid.Since) << 11
}

func (m *IdGenerator) GenSnowFlakeId() (int64, error) {
	id, err := m.snow.Next()
	return int64(id), err
}

func (m *IdGenerator) GetSnowFlakeIdStamp(sid int64) int64 {
	return gosnow.Since + sid>>22
}

func (m *IdGenerator) GetSnowFlakeIdWithStamp(stamp int64) int64 {
	return (stamp - gosnow.Since) << 22
}

func (m *IdGenerator) GenUuid() (string, error) {
	return sutil.GetUUID()
}

func (m *IdGenerator) GenUuidSha1() (string, error) {
	uuid, err := m.GenUuid()
	if err != nil {
		return "", err
	}

	h := sha1.Sum([]byte(uuid))
	return fmt.Sprintf("%x", h), nil
}

func (m *IdGenerator) GenUuidMd5() (string, error) {
	uuid, err := m.GenUuid()
	if err != nil {
		return "", err
	}

	h := md5.Sum([]byte(uuid))
	return fmt.Sprintf("%x", h), nil
}

//====================================
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

func genSid(client etcd.KeysAPI, path, skey string) (int, error) {
	fun := "genSid -->"
	r, err := client.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err != nil {
		return -1, err
	}

	js, _ := json.Marshal(r)

	slog.Infof("%s", js)

	if r.Node == nil || !r.Node.Dir {
		return -1, fmt.Errorf("node error location:%s", path)
	}

	slog.Infof("%s serv:%s len:%d", fun, r.Node.Key, r.Node.Nodes.Len())

	// 获取已有的servid，按从小到大排列
	ids := make([]int, 0)
	for _, n := range r.Node.Nodes {
		sid := n.Key[len(r.Node.Key)+1:]
		id, err := strconv.Atoi(sid)
		if err != nil || id < 0 {
			slog.Errorf("%s sid error key:%s", fun, n.Key)
		} else {
			ids = append(ids, id)
			if n.Value == skey {
				// 如果已经存在的sid使用的skey和设置一致，则使用之前的sid
				return id, nil
			}
		}
	}

	sort.Ints(ids)
	sid := 0
	for _, id := range ids {
		// 取不重复的最小的id
		if sid == id {
			sid++
		} else {
			break
		}
	}

	nserv := fmt.Sprintf("%s/%d", r.Node.Key, sid)
	r, err = client.Create(context.Background(), nserv, skey)
	if err != nil {
		return -1, err
	}

	jr, _ := json.Marshal(r)
	slog.Infof("%s newserv:%s rep:%s", fun, nserv, jr)

	return sid, nil

}

func retryGenSid(client etcd.KeysAPI, path, skey string, try int) (int, error) {
	fun := "retryGenSid -->"
	for i := 0; i < try; i++ {
		// 重试3次
		sid, err := genSid(client, path, skey)
		if err != nil {
			slog.Errorf("%s gensid try:%d path:%s err:%s", fun, i, path, err)
		} else {
			return sid, nil
		}
	}

	return -1, fmt.Errorf("gensid error try:%d", try)
}

func initSnowflake(servid int) (*gosnow.SnowFlake, error) {
	if servid < 0 {
		return nil, fmt.Errorf("init snowflake use nagtive servid")
	}
	gosnow.Since = time.Date(2014, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000
	v, err := gosnow.NewSnowFlake(uint32(servid))
	if err != nil {
		return nil, err
	}

	return v, nil
}

func initSlowid(servid int) (*slowid.Slowid, error) {
	if servid < 0 {
		return nil, fmt.Errorf("init snowflake use nagtive servid")
	}
	slowid.Since = time.Date(2014, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000
	v, err := slowid.NewSlowid(servid)
	if err != nil {
		return nil, err
	}

	return v, nil
}
