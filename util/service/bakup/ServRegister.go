package rocserv

import (
	"sync"
	"time"

	"github.com/shawnfeng/sutil/paconn"
	"roc/util/headope"
)

type servReginfo string {

	// 执行中，发生变化时候，变更
	Version uint64    `json:"version"`
	// type -> addr
	Service map[string]string  `json:"service"`
}

type ServRegister struct {

	// 启动前确认
	headAddrs []string
	servLocation string
	servSession string
	//------
	// 启动后确认
	servId int64
	// 以上数据一旦确认后，在整个程序运行周期内不在变更

	muServinfo  sync.Mutex
	servInfo servReginfo

}

func (m *ServRegister) headClose(a *paconn.Agent, data []byte, err error) error {

	return nil
}

func (m *ServRegister) headNotify(a *paconn.Agent, res []byte) []byte {


	return nil
}


func (m *ServRegister) liveHead() error {
	fun := "ServRegister.liveHead"
	master, err := headope.findMaster(m.headAddrs, time.Second * 10)
	if err != nil {
		return fmt.Errorf("%s get head master err:%s", fun, err)
	}
	slog.Infof("%s get head master:%s", fun, master)

	headAgent, err := paconn.NewAgentFromAddr(
		master
		time.Second*60*3,
		time.Second*60,,
		nil,
		m.headNotify,
		m.headClose,
	)

	if err != nil {
		return fmt.Errorf("%s Dial err:%s", fun, err)
	}


	// 1. 查找master，任何一个head都可以获取到master
	// 一个不行查另一个，直到查到为止，否则一直查，退避查
	// 2. 与master 建立连接
	// 3. 通过session 获取service id
	// 4. 增加update service 任务

	// 5. hold here until agent close, loop again

	// config 暂时不做实时的？

}


func (m *ServRegister) UpdateService(svType, svAddr string) {

}

func (m *ServRegister) Servid() {

}

// 获取自己service 对应location下对应的config
func (m *ServRegister) GetMyConfig(conf string) {

}

// 获取任意path的config
func (m *ServRegister) GetArbiConfig(conf string) {

}


func NewServRegisterWithId(headaddrs[]string, location string, session string, servid int64) *ServRegister {

}

func NewServRegister(headaddrs[]string, location string, session string) *ServRegister {

	reg := &ServRegister {
		headAddrs: headaddrs,
		servLocation: location,
		servSession: session,
	}


	return reg

}


// mutex



