package rocserv

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/util/servbase"

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
	if len(sb.crossRegisterRegionIds) == 0 {
		return initCrossRegisterCenterOrigin(sb)
	}

	return initCrossRegisterCenterNew(sb)
}

// 旧版本初始化跨机房注册etcd客户端, 使用服务内静态配置
func initCrossRegisterCenterOrigin(sb *ServBaseV2) error {
	fun := "initCrossRegisterCenterOrigin --> "
	ctx := context.Background()
	xlog.Infof(ctx, "%s start", fun)

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
			return fmt.Errorf("create etcd client failed, config: %v, err: %v", baseCfg, err)
		}
		baseKeysAPI := etcd.NewKeysAPI(baseClient) // not nil
		sb.crossRegisterClients[addr] = baseKeysAPI
	}

	xlog.Infof(ctx, "%s success", fun)
	return nil
}

// 新版本使用字符串格式的regionId代替addr作为client key
func initCrossRegisterCenterNew(sb *ServBaseV2) error {
	fun := "initCrossRegisterCenterNew --> "
	ctx := context.Background()
	xlog.Infof(ctx, "%s start", fun)

	for _, regionId := range sb.crossRegisterRegionIds {
		endpoints, ok := servbase.GetCrossRegisterEndpoints(regionId)
		if !ok {
			xlog.Errorf(ctx, "%s region has no endpoints, id: %d", fun, regionId)
			return fmt.Errorf("region has no endpoints, id: %d", regionId)
		}
		baseCfg := etcd.Config{
			Endpoints: endpoints,
			Transport: etcd.DefaultTransport,
		}
		baseClient, err := etcd.New(baseCfg)
		if err != nil {
			xlog.Errorf(ctx, "%s create etcd client failed, regionId: %v, config: %v, err: %v", fun, regionId, baseCfg, err)
			return fmt.Errorf("create etcd client failed, regionId: %v, config: %v, err: %v", regionId, baseCfg, err)
		}
		baseKeysAPI := etcd.NewKeysAPI(baseClient)

		regionIdStr := strconv.Itoa(regionId)
		sb.crossRegisterClients[regionIdStr] = baseKeysAPI
	}

	xlog.Infof(ctx, "%s success", fun)
	return nil
}
