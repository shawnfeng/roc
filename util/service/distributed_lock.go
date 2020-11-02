// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"fmt"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil/sync2"

	etcd "github.com/coreos/etcd/client"
)

const (
	TTL_LOCK = time.Second * 180
)

func (m *ServBaseV2) lookupLock(path string) *sync2.Semaphore {
	m.muLocks.Acquire()
	defer m.muLocks.Release()

	if mu, ok := m.locks[path]; ok {
		return mu
	} else {
		m.locks[path] = new(sync2.Semaphore)
		return m.locks[path]
	}

}

func (m *ServBaseV2) lockValue() string {
	return fmt.Sprintf("%s/%d:%s", m.servLocation, m.servId, m.sessKey)
}

func (m *ServBaseV2) resetExistLock(path string) error {
	fun := "ServBaseV2.resetExistLock -->"
	ctx := context.Background()

	// key 不存在返回类似:100: Key not found (/roc/lock) [6961237]
	// key 不相等返回类似:101: Compare failed ([7e07d3e6-2737-43ac-86fa-157bc1bb8943a != ttt]) [6962486]
	r, err := m.etcdClient.Set(context.Background(), path, m.lockValue(), &etcd.SetOptions{
		//PrevExist: etcd.PrevExist,
		PrevValue: m.lockValue(),
		TTL:       TTL_LOCK,
	})

	if err != nil {
		xlog.Infof(ctx, "%s exist check path: %s resp: %v err: %v", fun, path, r, err)
	} else {
		// 正常只有重启服务重新获取锁才会到这里
		xlog.Warnf(ctx, "%s exist check path: %s resp: %v", fun, path, r)
	}

	return err
}

func (m *ServBaseV2) setNoExistLock(path string) error {
	fun := "ServBaseV2.setNoExistLock -->"
	ctx := context.Background()
	r, err := m.etcdClient.Set(context.Background(), path, m.lockValue(), &etcd.SetOptions{
		PrevExist: etcd.PrevNoExist,
		TTL:       TTL_LOCK,
	})

	if err != nil {
		xlog.Warnf(ctx, "%s noexist check path: %s resp: %v err: %v", fun, path, r, err)
	} else {
		xlog.Infof(ctx, "%s noexist check path: %s resp: %v", fun, path, r)
	}

	return err

}

func (m *ServBaseV2) heartLock(path string) error {
	fun := "ServBaseV2.heartLock -->"
	ctx := context.Background()
	r, err := m.etcdClient.Set(context.Background(), path, "", &etcd.SetOptions{
		PrevExist: etcd.PrevExist,
		TTL:       TTL_LOCK,
		Refresh:   true,
	})

	if err != nil {
		xlog.Fatalf(ctx, "%s noexist heart path: %s resp: %v err: %v", fun, path, r, err)
	} else {
		xlog.Infof(ctx, "%s noexist heartpath: %s resp: %v", fun, path, r)
	}

	return err

}

func (m *ServBaseV2) delLock(path string) error {
	fun := "ServBaseV2.delLock -->"
	ctx := context.Background()
	r, err := m.etcdClient.Delete(context.Background(), path, &etcd.DeleteOptions{
		PrevValue: m.lockValue(),
	})
	// 100: Key not found (/roc/lock/local/niubi/fuck/testlock) [7044841]
	// 101: Compare failed ([7e07d3e6-2737-43ac-86fa-157bc1bb8943a != 332]) [7044908]
	if err != nil {
		xlog.Fatalf(ctx, "%s unlock path: %s resp: %v err: %v", fun, path, r, err)
	} else {
		xlog.Infof(ctx, "%s unlock path: %s resp: %v", fun, path, r)
	}

	return err
}

func (m *ServBaseV2) getDistLock(path string) error {
	fun := "ServBaseV2.getDistLock -->"
	ctx := context.Background()

	if err := m.resetExistLock(path); err == nil {
		return nil
	}
	// ===============================

	for {

		if err := m.setNoExistLock(path); err == nil {
			return nil
		}

		r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{})
		xlog.Infof(ctx, "%s get check path:%s resp:%v err:%v", fun, path, r, err)
		if err != nil {
			// 上面检查存在，这里又get不到，发生概率非常小
			xlog.Warnf(ctx, "%s little rate get check path:%s resp:%v err:%v", fun, path, r, err)
			continue
		}

		wop := &etcd.WatcherOptions{
			//AfterIndex: r.Node.ModifiedIndex+1,
			AfterIndex: r.Index,
		}
		watcher := m.etcdClient.Watcher(path, wop)
		if watcher == nil {
			xlog.Errorf(ctx, "%s get watcher get check path:%s err:%v", fun, path, err)
			return fmt.Errorf("get wather err")
		}

		xlog.Infof(ctx, "%s set watcher path:%s watcher:%v", fun, path, wop)

		r, err = watcher.Next(context.Background())
		xlog.Infof(ctx, "%s watchnext check path:%s resp:%v err:%v", fun, path, r, err)

		// 节点过期返回  expire {Key: /roc/lock/local/niubi/fuck/testlock, CreatedIndex: 7043099, ModifiedIndex: 7043144, TTL: 0

	}

}

//====================================
// 检查是不是首次获取，首次获取，可以认为是服务退出
// 又在锁没有失效的周期内重新启动了，这时候可以重新
// 由该服务副本优先获取到锁
// 同一个服务副本中多次在同一个path下调用lock，后续的会阻塞
func (m *ServBaseV2) lock(path string) error {
	m.lookupLock(path).Acquire()
	err := m.getDistLock(path)
	if err != nil {
		m.lookupLock(path).Release()
		return err
	}

	m.lookupHeart(path).start()
	return nil
}

func (m *ServBaseV2) unlock(path string) error {
	m.lookupHeart(path).stop()
	m.delLock(path)
	m.lookupLock(path).Release()
	return nil
}

func (m *ServBaseV2) trylock(path string) (bool, error) {
	fun := "ServBaseV2.trylock -->"
	ctx := context.Background()
	islock := m.lookupLock(path).TryAcquire()
	xlog.Infof(ctx, "%s try lock:%s r:%v", fun, path, islock)
	if !islock {
		return islock, nil
	}

	if err := m.resetExistLock(path); err == nil {
		m.lookupHeart(path).start()
		return true, nil
	}

	if err := m.setNoExistLock(path); err == nil {
		m.lookupHeart(path).start()
		return true, nil
	}

	m.lookupLock(path).Release()
	return false, nil
}

// 局部分布式锁 ======================
func (m *ServBaseV2) localLockPath(name string) string {
	return fmt.Sprintf("%s/%s/%s/%s", m.confEtcd.useBaseloc, BASE_LOC_LOCAL_DIST_LOCK, m.servLocation, name)
}

func (m *ServBaseV2) Lock(name string) error {
	if m.isPreEnvGroup() {
		<-(chan int)(nil)
		return fmt.Errorf("pre environment cannot acquire the lock")
	}

	return m.lock(m.localLockPath(name))
}

func (m *ServBaseV2) Unlock(name string) error {
	if m.isPreEnvGroup() {
		return fmt.Errorf("pre environment cannot acquire the lock")
	}

	return m.unlock(m.localLockPath(name))
}

func (m *ServBaseV2) Trylock(name string) (bool, error) {
	if m.isPreEnvGroup() {
		return false, nil
	}

	return m.trylock(m.localLockPath(name))
}

// 全局分布式锁=======================
func (m *ServBaseV2) globalLockPath(name string) string {
	return fmt.Sprintf("%s/%s/%s", m.confEtcd.useBaseloc, BASE_LOC_GLOBAL_DIST_LOCK, name)
}

func (m *ServBaseV2) LockGlobal(name string) error {
	if m.isPreEnvGroup() {
		<-(chan int)(nil)
		return fmt.Errorf("pre environment cannot acquire the lock")
	}

	return m.lock(m.globalLockPath(name))
}

func (m *ServBaseV2) UnlockGlobal(name string) error {
	if m.isPreEnvGroup() {
		return fmt.Errorf("pre environment cannot acquire the lock")
	}

	return m.unlock(m.globalLockPath(name))
}

func (m *ServBaseV2) TrylockGlobal(name string) (bool, error) {
	if m.isPreEnvGroup() {
		return false, nil
	}

	return m.trylock(m.globalLockPath(name))
}

func (m *ServBaseV2) lookupHeart(path string) *distLockHeart {
	m.muHearts.Acquire()
	defer m.muHearts.Release()

	if mu, ok := m.hearts[path]; ok {
		return mu
	} else {
		m.hearts[path] = newdistLockHeart(m, path)
		return m.hearts[path]
	}

}

// 分布式锁心跳控制器
// ==========================
type distLockHeart struct {
	path  string
	sb    *ServBaseV2
	onoff chan bool
}

func newdistLockHeart(sb *ServBaseV2, path string) *distLockHeart {
	r := &distLockHeart{
		sb:    sb,
		path:  path,
		onoff: make(chan bool),
	}

	go r.loop()

	return r
}

func (m *distLockHeart) loop() {
	fun := "distLockHeart.loop -->"
	ctx := context.Background()
	var ison bool
	tick := time.NewTicker(time.Second * 20)

	for {
		select {
		case <-tick.C:
			xlog.Infof(ctx, "%s heart check path:%s ison:%v", fun, m.path, ison)
			if ison {
				m.sb.heartLock(m.path)
			}

		case v := <-m.onoff:
			xlog.Infof(ctx, "%s onoff path:%s ison:%v", fun, m.path, v)
			ison = v
		}
	}
}

func (m *distLockHeart) start() {
	fun := "distLockHeart.start -->"
	xlog.Infof(context.Background(), "%s heart check path:%s start", fun, m.path)
	m.onoff <- true
}

func (m *distLockHeart) stop() {
	fun := "distLockHeart.stop -->"
	xlog.Infof(context.Background(), "%s heart check path:%s stop", fun, m.path)
	m.onoff <- false
}
