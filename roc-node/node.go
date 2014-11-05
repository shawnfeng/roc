package main

import (
	"time"
	"strings"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/paconn"

	"roc/roc-node/jobs"
)

type nodeMon struct {
	agm *paconn.AgentManager
	mon *jobs.Job
	jobm *jobs.JobManager

}

func (m *nodeMon) cbNew(a *paconn.Agent) {
	fun := "nodeMon.cbNew"
	slog.Infof("%s a:%s", fun, a)
}

func (m *nodeMon) allJobs() []byte {
	fun := "nodeMon.allJobs"

	allrunjobs := m.jobm.Runjobs()
	sall := strings.Join(allrunjobs, ",")

	slog.Infof("%s alljobs:%s", fun, sall)

	return []byte(sall)
}

func (m *nodeMon) jobChanges(j *jobs.Job) {

	fun := "nodeMon.jobChanges"

	sall := m.allJobs()

	agents := m.agm.Agents()
	for _, ag := range(agents) {
		res, err := ag.Twoway(sall, 200)
		if err != nil {
			slog.Errorf("%s notify ag:%s err:%s", fun, ag, err)
		}

		if string(res) != "OK" {
			slog.Errorf("%s notify ag:%s res:%s", fun, ag, res)
		}
		
	}

}

func (m *nodeMon) cbTwoway(a *paconn.Agent, req []byte) []byte {
	if string(req) == "GET JOBS" {
		return m.allJobs()
	} else {
		return nil
	}
}

func (m *nodeMon) cbClose(a *paconn.Agent, pack []byte, err error) {
	fun := "nodeMon.cbClose"
	slog.Infof("%s a:%s pack:%v err:%v", fun, a, pack, err)

}

func (m *nodeMon) UpdateJobs(confs map[string]*jobs.ManulConf) {
	m.jobm.Update(confs)
}

func (m *nodeMon) StartJob(jobid string) error {
	return m.jobm.Start(jobid)
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

	// job manager
	jobm := jobs.NewJobManager(
		m.jobChanges,
		m.jobChanges,
	)
	m.jobm = jobm



	// start node-monitor
	mc := &jobs.ManulConf {
		"node-monitor",
		"./roc-node-monitor",
		[]string{m.agm.Listenport()},
		true,
		time.Millisecond*100,
	}


	m.mon = jobs.Newjob(mc, nil, nil)
	m.mon.Start()

}

func main() {
	fun := "main"
	nm := &nodeMon {
	}

	nm.Init()


	/////////////////////////////////////
	m0 := &jobs.ManulConf {
		"job0",
		"sh",
		[]string{"c.sh"},
		true,
		time.Second*20,
	}


	m1 := &jobs.ManulConf {
		"job1",
		"sh",
		[]string{"c_loop.sh"},
		true,
		time.Second*20,
	}
	

	nm.UpdateJobs(
		map[string]*jobs.ManulConf {
			"job0": m0,
			"job1": m1,
		},
	)

	time.Sleep(time.Second * 15)

	err := nm.StartJob("job0")
	if err != nil {
		slog.Errorf("%s start job0 err:%s", fun, err)
	}


	err = nm.StartJob("job1")
	if err != nil {
		slog.Errorf("%s start job1 err:%s", fun, err)
	}


	err = nm.StartJob("jobx")
	if err != nil {
		slog.Errorf("%s start jobx err:%s", fun, err)
	}



	time.Sleep(time.Second * 60*5)

}
