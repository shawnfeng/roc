// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"fmt"
	"time"

	etcd "github.com/coreos/etcd/client"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/ssync"

	"golang.org/x/net/context"
)

const (
	TTL_LOCK = time.Second * 180
)

func (m *ServBaseV2) lookupLock(path string) *ssync.Mutex {
	m.muLocks.Lock()
	defer m.muLocks.Unlock()

	if mu, ok := m.locks[path]; ok {
		return mu
	} else {
		m.locks[path] = new(ssync.Mutex)
		return m.locks[path]
	}

}

func (m *ServBaseV2) lockValue() string {
	return fmt.Sprintf("%s/%d:%s", m.servLocation, m.servId, m.sessKey)
}

func (m *ServBaseV2) resetExistLock(path string) error {
	fun := "ServBaseV2.resetExistLock -->"

	// key 不存在返回类似:100: Key not found (/roc/lock) [6961237]
	// key 不相等返回类似:101: Compare failed ([7e07d3e6-2737-43ac-86fa-157bc1bb8943a != ttt]) [6962486]
	r, err := m.etcdClient.Set(context.Background(), path, m.lockValue(), &etcd.SetOptions{
		//PrevExist: etcd.PrevExist,
		PrevValue: m.lockValue(),
		TTL:       TTL_LOCK,
	})

	if err != nil {
		slog.Infof("%s exist check path:%s resp:%v err:%v", fun, path, r, err)
	} else {
		// 正常只有重启服务重新获取锁才会到这里
		slog.Warnf("%s exist check path:%s resp:%v", fun, path, r)
	}

	return err
}

func (m *ServBaseV2) setNoExistLock(path string) error {
	fun := "ServBaseV2.setNoExistLock -->"

	r, err := m.etcdClient.Set(context.Background(), path, m.lockValue(), &etcd.SetOptions{
		PrevExist: etcd.PrevNoExist,
		TTL:       TTL_LOCK,
	})

	if err != nil {
		slog.Warnf("%s noexist check path:%s resp:%v err:%v", fun, path, r, err)
	} else {
		slog.Infof("%s noexist check path:%s resp:%v", fun, path, r)
	}

	return err

}

func (m *ServBaseV2) heartLock(path string) error {
	fun := "ServBaseV2.heartLock -->"

	r, err := m.etcdClient.Set(context.Background(), path, "", &etcd.SetOptions{
		PrevExist: etcd.PrevExist,
		TTL:       TTL_LOCK,
		Refresh:   true,
	})

	if err != nil {
		slog.Fatalf("%s noexist heart path:%s resp:%v err:%v", fun, path, r, err)
	} else {
		slog.Infof("%s noexist heartpath:%s resp:%v", fun, path, r)
	}

	return err

}

func (m *ServBaseV2) delLock(path string) error {
	fun := "ServBaseV2.delLock -->"
	r, err := m.etcdClient.Delete(context.Background(), path, &etcd.DeleteOptions{
		PrevValue: m.lockValue(),
	})
	// 100: Key not found (/roc/lock/local/niubi/fuck/testlock) [7044841]
	// 101: Compare failed ([7e07d3e6-2737-43ac-86fa-157bc1bb8943a != 332]) [7044908]
	if err != nil {
		slog.Fatalf("%s unlock path:%s resp:%v err:%v", fun, path, r, err)
	} else {
		slog.Infof("%s unlock path:%s resp:%v", fun, path, r)
	}

	return err
}

func (m *ServBaseV2) getDistLock(path string) error {
	fun := "ServBaseV2.getDistLock -->"

	if err := m.resetExistLock(path); err == nil {
		return nil
	}
	// ===============================

	for {

		if err := m.setNoExistLock(path); err == nil {
			return nil
		}

		r, err := m.etcdClient.Get(context.Background(), path, &etcd.GetOptions{})
		slog.Infof("%s get check path:%s resp:%v err:%v", fun, path, r, err)
		if err != nil {
			// 上面检查存在，这里又get不到，发生概率非常小
			slog.Warnf("%s little rate get check path:%s resp:%v err:%v", fun, path, r, err)
			continue
		}

		wop := &etcd.WatcherOptions{
			//AfterIndex: r.Node.ModifiedIndex+1,
			AfterIndex: r.Index,
		}
		watcher := m.etcdClient.Watcher(path, wop)
		if watcher == nil {
			slog.Errorf("%s get watcher get check path:%s err:%v", fun, path, err)
			return fmt.Errorf("get wather err")
		}

		slog.Infof("%s set watcher path:%s watcher:%v", fun, path, wop)

		r, err = watcher.Next(context.Background())
		slog.Infof("%s watchnext check path:%s resp:%v err:%v", fun, path, r, err)

		// 节点过期返回  expire {Key: /roc/lock/local/niubi/fuck/testlock, CreatedIndex: 7043099, ModifiedIndex: 7043144, TTL: 0

	}

}

//====================================
// 检查是不是首次获取，首次获取，可以认为是服务退出
// 又在锁没有失效的周期内重新启动了，这时候可以重新
// 由该服务副本优先获取到锁
// 同一个服务副本中多次在同一个path下调用lock，后续的会阻塞
func (m *ServBaseV2) lock(path string) error {
	m.lookupLock(path).Lock()
	err := m.getDistLock(path)
	if err != nil {
		m.lookupLock(path).Unlock()
		return err
	}

	m.lookupHeart(path).start()
	return nil
}

func (m *ServBaseV2) unlock(path string) error {
	m.lookupHeart(path).stop()
	m.delLock(path)
	m.lookupLock(path).Unlock()
	return nil
}

func (m *ServBaseV2) trylock(path string) (bool, error) {
	fun := "ServBaseV2.trylock -->"

	islock := m.lookupLock(path).Trylock()
	slog.Infof("%s try lock:%s r:%v", fun, path, islock)
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

	m.lookupLock(path).Unlock()
	return false, nil
}

// 局部分布式锁 ======================
func (m *ServBaseV2) localLockPath(name string) string {
	return fmt.Sprintf("%s/%s/%s/%s", m.confEtcd.useBaseloc, BASE_LOC_LOCAL_DIST_LOCK, m.servLocation, name)
}

func (m *ServBaseV2) Lock(name string) error {
	return m.lock(m.localLockPath(name))
}

func (m *ServBaseV2) Unlock(name string) error {
	return m.unlock(m.localLockPath(name))
}

func (m *ServBaseV2) Trylock(name string) (bool, error) {
	return m.trylock(m.localLockPath(name))
}

// 全局分布式锁=======================
func (m *ServBaseV2) globalLockPath(name string) string {
	return fmt.Sprintf("%s/%s/%s", m.confEtcd.useBaseloc, BASE_LOC_GLOBAL_DIST_LOCK, name)
}

func (m *ServBaseV2) LockGlobal(name string) error {
	return m.lock(m.globalLockPath(name))
}

func (m *ServBaseV2) UnlockGlobal(name string) error {
	return m.unlock(m.globalLockPath(name))

}

func (m *ServBaseV2) TrylockGlobal(name string) (bool, error) {
	return m.trylock(m.globalLockPath(name))
}

func (m *ServBaseV2) lookupHeart(path string) *distLockHeart {
	m.muHearts.Lock()
	defer m.muHearts.Unlock()

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
	var ison bool
	tick := time.NewTicker(time.Second * 20)

	for {
		select {
		case <-tick.C:
			slog.Infof("%s heart check path:%s ison:%v", fun, m.path, ison)
			if ison {
				m.sb.heartLock(m.path)
			}

		case v := <-m.onoff:
			slog.Infof("%s onoff path:%s ison:%v", fun, m.path, v)
			ison = v
		}
	}
}

func (m *distLockHeart) start() {
	fun := "distLockHeart.start -->"
	slog.Infof("%s heart check path:%s start", fun, m.path)
	m.onoff <- true
}

func (m *distLockHeart) stop() {
	fun := "distLockHeart.stop -->"
	slog.Infof("%s heart check path:%s stop", fun, m.path)
	m.onoff <- false
}
