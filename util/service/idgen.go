package rocserv

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xutil"

	etcd "github.com/coreos/etcd/client"
	"github.com/sdming/gosnow"
	"github.com/shawnfeng/sutil/slowid"
)

// IdGenerator id生成逻辑, will move to idgen service
type IdGenerator struct {
	workerID int
	mu       sync.Mutex
	slow     map[string]*slowid.Slowid
	snow     *gosnow.SnowFlake
}

func (m *IdGenerator) GenSlowId(tp string) (int64, error) {
	gslow := func(tp string) (*slowid.Slowid, error) {
		m.mu.Lock()
		defer m.mu.Unlock()

		s := m.slow[tp]
		if s == nil {
			sl, err := initSlowid(m.workerID)
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

// GetSlowIdStamp return timestamp part of slow id, will move to idgen service
func (m *IdGenerator) GetSlowIdStamp(id int64) int64 {
	return slowid.Since + id>>11
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
	return xutil.GetUUID()
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

func genSid(client etcd.KeysAPI, path, skey string) (int, error) {
	fun := "genSid -->"
	ctx := context.Background()
	r, err := client.Get(context.Background(), path, &etcd.GetOptions{Recursive: true, Sort: false})
	if err != nil {
		return -1, err
	}

	js, _ := json.Marshal(r)

	xlog.Infof(ctx, "%s", js)

	if r.Node == nil || !r.Node.Dir {
		return -1, fmt.Errorf("node error location:%s", path)
	}

	xlog.Infof(ctx, "%s serv:%s len:%d", fun, r.Node.Key, r.Node.Nodes.Len())

	// 获取已有的servid，按从小到大排列
	ids := make([]int, 0)
	for _, n := range r.Node.Nodes {
		sid := n.Key[len(r.Node.Key)+1:]
		id, err := strconv.Atoi(sid)
		if err != nil || id < 0 {
			xlog.Errorf(ctx, "%s sid error key:%s", fun, n.Key)
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
	xlog.Infof(ctx, "%s newserv:%s resp:%s", fun, nserv, jr)

	return sid, nil

}

func retryGenSid(client etcd.KeysAPI, path, skey string, try int) (int, error) {
	fun := "retryGenSid -->"
	ctx := context.Background()
	for i := 0; i < try; i++ {
		// 重试3次
		sid, err := genSid(client, path, skey)
		if err != nil {
			xlog.Errorf(ctx, "%s gensid try: %d path: %s err: %v", fun, i, path, err)
		} else {
			return sid, nil
		}
	}

	return -1, fmt.Errorf("gensid error try:%d", try)
}

func initSnowflake(sid int) (*gosnow.SnowFlake, error) {
	if sid < 0 {
		return nil, fmt.Errorf("init snowflake use nagtive servid")
	}
	gosnow.Since = time.Date(2014, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000
	v, err := gosnow.NewSnowFlake(uint32(sid))
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
