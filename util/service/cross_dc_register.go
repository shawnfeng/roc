package rocserv

import (
	"context"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"

	etcd "github.com/coreos/etcd/client"
)

// RegisterCrossDCService, the path and value is the same as RegisterService, but different register center
func (m *ServBaseV2) RegisterCrossDCService(servs map[string]*ServInfo) error {
	fun := "ServBaseV2.RegisterService -->"
	ctx := context.Background()
	err := m.RegisterServiceV2(servs, BASE_LOC_REG_SERV, true)
	if err != nil {
		xlog.Errorf(ctx, "%s register server v2 failed, err: %v", fun, err)
		return err
	}

	err = m.RegisterServiceV1(servs, true)
	if err != nil {
		xlog.Errorf(ctx, "%s register server v1 failed, err: %v", fun, err)
		return err
	}

	xlog.Infof(ctx, "%s register cross dc server ok", fun)

	return nil
}

func (m *ServBaseV2) doCrossDCRegister(path, js string, refresh bool) error {
	fun := "ServBaseV2.doCrossDCRegister -->"
	ctx := context.Background()
	for addr, _ := range m.crossRegisterClients {
		// 创建完成标志
		var isCreated bool

		go func() {

			for j := 0; ; j++ {
				updateEtcd := func() {
					var err error
					var r *etcd.Response
					if !isCreated {
						xlog.Warnf(ctx, "%s create idx:%d server_info: %s", fun, j, js)
						r, err = m.crossRegisterClients[addr].Set(context.Background(), path, js, &etcd.SetOptions{
							TTL: time.Second * 60,
						})
					} else {
						if refresh {
							// 在刷新ttl时候，不允许变更value
							r, err = m.crossRegisterClients[addr].Set(context.Background(), path, "", &etcd.SetOptions{
								PrevExist: etcd.PrevExist,
								TTL:       time.Second * 60,
								Refresh:   true,
							})
						} else {
							r, err = m.crossRegisterClients[addr].Set(context.Background(), path, js, &etcd.SetOptions{
								TTL: time.Second * 60,
							})
						}

					}

					if err != nil {
						isCreated = false
						xlog.Errorf(ctx, "%s reg idx: %d, resp: %v, err: %v", fun, j, r, err)

					} else {
						isCreated = true
					}
				}

				withRegLockRunClosureBeforeStop(m, ctx, fun, updateEtcd)

				time.Sleep(time.Second * 20)

				if m.isStop() {
					xlog.Infof(ctx, "%s server stop, register info [%s] clear", fun, path)
					return
				}
			}

		}()
	}

	return nil
}

func (m *ServBaseV2) clearCrossDCRegisterInfos() {
	fun := "ServBaseV2.clearCrossDCRegisterInfos -->"
	ctx := context.Background()
	//延迟清理注册信息,防止新实例还没有完成注册
	time.Sleep(time.Second * 2)

	m.muReg.Lock()
	defer m.muReg.Unlock()

	for addr, _ := range m.crossRegisterClients {
		for path, _ := range m.regInfos {
			_, err := m.crossRegisterClients[addr].Delete(context.Background(), path, &etcd.DeleteOptions{
				Recursive: true,
			})
			if err != nil {
				xlog.Warnf(ctx, "%s path: %s, err: %v", fun, path, err)
			}
		}
	}
}

// 初始化跨机房etcd客户端
func initCrossRegisterCenter(sb *ServBaseV2) error {
	var baseConfig BaseConfig
	err := sb.ServConfig(&baseConfig)
	if err != nil {
		return err
	}
	for _, addr := range baseConfig.Base.CrossRegisterCenters {
		baseCfg := etcd.Config{
			Endpoints: []string{addr},
			Transport: etcd.DefaultTransport,
		}
		baseClient, err := etcd.New(baseCfg)
		if err != nil {
			return fmt.Errorf("create etcd client failed, config: %v", baseCfg)
		}
		baseKeysAPI := etcd.NewKeysAPI(baseClient)
		if baseClient == nil {
			return fmt.Errorf("create etchd api error")
		}
		sb.crossRegisterClients[addr] = baseKeysAPI
	}
	return nil
}
