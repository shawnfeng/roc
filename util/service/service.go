// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig/apollo"
	xmgo "gitlab.pri.ibanyu.com/middleware/seaweed/xmgo/manager"
	xsql "gitlab.pri.ibanyu.com/middleware/seaweed/xsql/manager"

	etcd "github.com/coreos/etcd/client"
	"github.com/shawnfeng/sutil/dbrouter"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/slowid"
	"github.com/shawnfeng/sutil/ssync"
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

	// thrift 服务注册的位置
	BASE_LOC_REG_SERV = "serve"

	// 后门注册的位置
	BASE_LOC_REG_BACKDOOR = "backdoor"

	// 服务手动配置位置
	BASE_LOC_REG_MANUAL = "manual"
	// sla metrics注册的位置
	BASE_LOC_REG_METRICS = "metrics"

	PROCESSOR_GRPC_PROPERTY_NAME = "proc_grpc"

	PROCESSOR_THRIFT_PROPERTY_NAME = "proc_thrift"

	//预演环境分组标识
	ENV_GROUP_PRE = "pre"

	// RPCConfNamespace RPC Apollo Conf Namespace
	RPCConfNamespace = "rpc.client"
)

type configEtcd struct {
	etcdAddrs  []string
	useBaseloc string
}

type ServBaseV2 struct {
	IdGenerator

	confEtcd     configEtcd
	configCenter xconfig.ConfigCenter

	dbLocation   string
	servLocation string
	servGroup    string
	servName     string
	copyName     string
	sessKey      string

	envGroup string

	etcdClient etcd.KeysAPI

	// 跨机房服务注册
	crossRegisterClients map[string]etcd.KeysAPI

	servId int

	dbRouter *dbrouter.Router

	muLocks ssync.Mutex
	locks   map[string]*ssync.Mutex

	muHearts ssync.Mutex
	hearts   map[string]*distLockHeart

	muStop sync.Mutex
	stop   bool

	muReg    sync.Mutex
	regInfos map[string]string
}

func (m *ServBaseV2) isStop() bool {
	m.muStop.Lock()
	defer m.muStop.Unlock()

	return m.stop
}

func (m *ServBaseV2) Stop() {
	m.setStatusToStop()
	m.clearRegisterInfos()
	m.clearCrossDCRegisterInfos()
}

func (m *ServBaseV2) setStatusToStop() {
	m.muStop.Lock()
	defer m.muStop.Unlock()

	m.stop = true
}

func (m *ServBaseV2) addRegisterInfo(path, regInfo string) {
	m.muReg.Lock()
	defer m.muReg.Unlock()

	m.regInfos[path] = regInfo
}

func (m *ServBaseV2) clearRegisterInfos() {
	fun := "ServBaseV2.clearRegisterInfos -->"

	//延迟清理注册信息,防止新实例还没有完成注册
	time.Sleep(time.Second * 2)

	m.muReg.Lock()
	defer m.muReg.Unlock()

	for path, _ := range m.regInfos {
		_, err := m.etcdClient.Delete(context.Background(), path, &etcd.DeleteOptions{
			Recursive: true,
		})
		if err != nil {
			slog.Warnf("%s path: %s, err: %v", fun, path, err)
		}
	}
}

func (m *ServBaseV2) RegisterBackDoor(servs map[string]*ServInfo) error {
	rd := &RegData{
		Servs: servs,
	}

	js, err := json.Marshal(rd)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, BASE_LOC_REG_BACKDOOR)

	return m.doRegister(path, string(js), true)
}

func (m *ServBaseV2) RegisterMetrics(servs map[string]*ServInfo) error {
	rd := &RegData{
		Servs: servs,
	}

	js, err := json.Marshal(rd)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, BASE_LOC_REG_METRICS)

	return m.doRegister(path, string(js), true)
}

// {type:http/thrift, addr:10.3.3.3:23233, processor:fuck}
func (m *ServBaseV2) RegisterService(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterService -->"

	err := m.RegisterServiceV2(servs, BASE_LOC_REG_SERV, false)
	if err != nil {
		slog.Errorf("%s register server v2 failed, err: %v", fun, err)
		return err
	}

	err = m.RegisterServiceV1(servs, false)
	if err != nil {
		slog.Errorf("%s register server v1 failed, err: %v", fun, err)
		return err
	}

	slog.Infof("%s register server ok", fun)

	return nil
}

func (m *ServBaseV2) RegisterServiceV2(servs map[string]*ServInfo, dir string, crossDC bool) error {
	rd := &RegData{
		Servs: servs,
	}

	js, err := json.Marshal(rd)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, dir)

	// 非跨机房
	if !crossDC {
		return m.doRegister(path, string(js), true)
	}
	// 跨机房
	return m.doCrossDCRegister(path, string(js), true)
}

// 为兼容老的client发现服务，保留的
func (m *ServBaseV2) RegisterServiceV1(servs map[string]*ServInfo, crossDC bool) error {
	fun := "ServBaseV2.RegisterServiceV1 -->"

	js, err := json.Marshal(servs)
	if err != nil {
		return err
	}

	slog.Infof("%s servs:%s", fun, js)

	path := fmt.Sprintf("%s/%s/%s/%d", m.confEtcd.useBaseloc, BASE_LOC_DIST, m.servLocation, m.servId)

	// 非跨机房
	if !crossDC {
		return m.doRegister(path, string(js), true)
	}
	// 跨机房
	return m.doCrossDCRegister(path, string(js), true)
}

func (m *ServBaseV2) SetGroupAndDisable(group string, disable bool) error {
	fun := "ServBaseV2.SetGroupAndDisable -->"

	path := fmt.Sprintf("%s/%s/%s/%d/%s", m.confEtcd.useBaseloc, BASE_LOC_DIST_V2, m.servLocation, m.servId, BASE_LOC_REG_MANUAL)
	value, err := m.getValueFromEtcd(path)
	if err != nil {
		slog.Warnf("%s getValueFromEtcd err, path:%s, err:%v", fun, path, err)
	}

	manual := &ManualData{}
	err = json.Unmarshal([]byte(value), manual)
	if len(value) > 0 && err != nil {
		slog.Errorf("%s unmarshal err, value:%s, err:%v", fun, value, err)
		return err
	}

	if manual.Ctrl == nil {
		manual.Ctrl = &ServCtrl{}
	}

	isFind := false
	for _, g := range manual.Ctrl.Groups {
		if g == group {
			isFind = true
			break
		}
	}

	if isFind == false {
		manual.Ctrl.Groups = append(manual.Ctrl.Groups, group)
	}
	if manual.Ctrl.Weight == 0 {
		manual.Ctrl.Weight = 100
	}
	manual.Ctrl.Disable = disable

	newValue, err := json.Marshal(manual)
	if err != nil {
		slog.Errorf("%s marshal err, manual:%v, err:%v", fun, manual, err)
		return err
	}

	slog.Infof("%s path:%s old value:%s new value:%s", fun, path, value, newValue)
	err = m.setValueToEtcd(path, string(newValue), nil)
	if err != nil {
		slog.Errorf("%s setValueToEtcd err, path:%s value:%s", fun, path, newValue)
	}

	return err
}

func (m *ServBaseV2) getValueFromEtcd(path string) (value string, err error) {
	fun := "ServBaseV2.getValueFromEtcd -->"

	r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{Recursive: false, Sort: false})
	if err != nil {
		slog.Warnf("%s path:%s err:%v", fun, path, err)
		return "", err
	}

	for _, n := range r.Node.Nodes {
		for _, nc := range n.Nodes {
			return nc.Value, nil
		}
	}

	return "", nil
}

func (m *ServBaseV2) setValueToEtcd(path, value string, opts *etcd.SetOptions) error {
	fun := "ServBaseV2.setValueToEtcd -->"

	_, err := m.etcdClient.Set(context.Background(), path, value, opts)
	if err != nil {
		slog.Errorf("%s path:%s value:%s opts:%v", fun, path, value, opts)
	}

	return err
}

func (m *ServBaseV2) doRegister(path, js string, refresh bool) error {
	fun := "ServBaseV2.doRegister -->"
	m.addRegisterInfo(path, js)

	// 创建完成标志
	var isCreated bool

	go func() {
		for i := 0; ; i++ {
			var err error
			if !isCreated {
				slog.Warnf("%s create node, round: %d server_info: %s", fun, i, js)
				_, err = m.etcdClient.Set(context.Background(), path, js, &etcd.SetOptions{
					TTL: time.Second * 60,
				})
			} else {
				if refresh {
					// 在刷新ttl时候，不允许变更value
					_, err = m.etcdClient.Set(context.Background(), path, "", &etcd.SetOptions{
						PrevExist: etcd.PrevExist,
						TTL:       time.Second * 60,
						Refresh:   true,
					})
				} else {
					_, err = m.etcdClient.Set(context.Background(), path, js, &etcd.SetOptions{
						TTL: time.Second * 60,
					})
				}

			}

			if err != nil {
				isCreated = false
				slog.Warnf("%s register need create node, round: %d, err: %v", fun, i, err)

			} else {
				isCreated = true
			}

			time.Sleep(time.Second * 20)

			if m.isStop() {
				slog.Infof("%s server stop, register path [%s] clear", fun, path)
				return
			}
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

// ConfigCenter ...
func (m *ServBaseV2) ConfigCenter() xconfig.ConfigCenter {
	return m.configCenter
}

// RegInfos ...
func (m *ServBaseV2) RegInfos() map[string]string {
	m.muReg.Lock()
	defer m.muReg.Unlock()
	return m.regInfos
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

	tf := xconfig.NewTierConf()
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
func NewServBaseV2(confEtcd configEtcd, servLocation, skey, envGroup string, sidOffset int) (*ServBaseV2, error) {
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

	slog.Infof("%s path:%s sid:%d skey:%s, envGroup", fun, path, sid, skey, envGroup)

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

	configCenter, err := xconfig.NewConfigCenter(context.TODO(), apollo.ConfigTypeApollo, servLocation, []string{RPCConfNamespace, xsql.MysqlConfNamespace, xmgo.MongoConfNamespace})
	if err != nil {
		return nil, err
	}

	reg := &ServBaseV2{
		confEtcd:             confEtcd,
		dbLocation:           dbloc,
		servLocation:         servLocation,
		sessKey:              skey,
		etcdClient:           client,
		crossRegisterClients: make(map[string]etcd.KeysAPI, 2),
		servId:               sid,
		locks:                make(map[string]*ssync.Mutex),
		hearts:               make(map[string]*distLockHeart),
		regInfos:             make(map[string]string),

		dbRouter:     dr,
		configCenter: configCenter,

		envGroup: envGroup,
	}

	svrInfo := strings.SplitN(servLocation, "/", 2)
	if len(svrInfo) == 2 {
		reg.servGroup = svrInfo[0]
		reg.servName = svrInfo[1]
	} else {
		slog.Warnf("%s servLocation:%s do not match group/service format", fun, servLocation)
	}

	sf, err := initSnowflake(sid + sidOffset)
	if err != nil {
		return nil, err
	}

	reg.IdGenerator.snow = sf
	reg.IdGenerator.slow = make(map[string]*slowid.Slowid)
	reg.IdGenerator.workerID = sid + sidOffset

	// init cross register clients
	err = initCrossRegisterCenter(reg)
	if err != nil {
		return nil, err
	}

	return reg, nil

}

func (m *ServBaseV2) isPreEnvGroup() bool {
	if m.envGroup == ENV_GROUP_PRE {
		return true
	}

	return false
}

// mutex
