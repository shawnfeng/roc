package main

import (
	"time"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/paconn"

	"roc/roc-node/jobs"
)

type nodeMon struct {
	agm *paconn.AgentManager
	mon *jobs.Job

}

func (m *nodeMon) cbNew(a *paconn.Agent) {
	fun := "nodeMon.cbNew"
	slog.Infof("%s a:%s", fun, a)
}

func (m *nodeMon) cbTwoway(a *paconn.Agent, req []byte) []byte {
	if string(req) == "GET JOBS" {
		return []byte("")
	} else {
		return nil
	}
}

func (m *nodeMon) cbClose(a *paconn.Agent, pack []byte, err error) {
	fun := "nodeMon.cbClose"
	slog.Infof("%s a:%s pack:%v err:%v", fun, a, pack, err)

}


func (m *nodeMon) Init() {

	fun := "nodeMon.Init"

	agm, err := paconn.NewAgentManager(
		":",
		time.Second * 60 *15,
		0,
		m.cbNew,
		nil,
		m.cbTwoway,
		m.cbClose,

	)

	if err != nil {
		slog.Panicf("%s err:%s", fun, err)
	}

	slog.Infof("%s %s", fun, agm.Listenport())
	m.agm = agm

	// ----------
	mc := &jobs.ManulConf {
		"node-monitor",
		"./roc-node-monitor",
		[]string{m.agm.Listenport()},
		true,
		time.Millisecond*100,
	}


	m.mon = jobs.Newjob(mc)
	m.mon.Start()

}

func main() {
	nm := &nodeMon {

	}

	nm.Init()

	time.Sleep(time.Second * 60*5)


}
