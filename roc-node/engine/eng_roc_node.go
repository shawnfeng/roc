// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package engine

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shawnfeng/sutil"
	"github.com/shawnfeng/sutil/paconn"
	"github.com/shawnfeng/sutil/sconf"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/snetutil"

	"github.com/shawnfeng/roc/roc-node/jobs"
)

type nodeMon struct {
	agm  *paconn.AgentManager
	mon  *jobs.Job
	jobm *jobs.JobManager

	// 关闭monitor监控的job
	muMond     sync.Mutex
	monDisable map[string]bool
}

func (m *nodeMon) upMonitorDisableList(list []string) {
	fun := "nodeMon.upMonitorDisableList"
	slog.Infof("%s list:%s", fun, list)

	nmon := make(map[string]bool)
	for _, d := range list {
		nmon[d] = true
	}

	m.muMond.Lock()
	defer m.muMond.Unlock()

	m.monDisable = nmon
}

func (m *nodeMon) filtMonitorDisable(jobs map[string]string) []string {

	m.muMond.Lock()
	defer m.muMond.Unlock()

	rv := make([]string, 0)
	for id, pid := range jobs {
		if _, ok := m.monDisable[id]; !ok {
			rv = append(rv, pid)
		}
	}

	return rv

}

func (m *nodeMon) cbNew(a *paconn.Agent) {
	fun := "nodeMon.cbNew"
	slog.Infof("%s a:%s", fun, a)
}

func (m *nodeMon) allJobs() []byte {
	fun := "nodeMon.allJobs"

	allrunjobs := m.jobm.Runjobs()

	filtjobs := m.filtMonitorDisable(allrunjobs)

	sall := strings.Join(filtjobs, ",")

	slog.Infof("%s alljobs:%s", fun, sall)

	return []byte(sall)
}

func (m *nodeMon) jobChanges(pid int32, j *jobs.Job) {

	fun := "nodeMon.jobChanges"

	sall := m.allJobs()

	agents := m.agm.Agents()
	for _, ag := range agents {
		_, res, err := ag.Twoway(0, sall, 200*time.Millisecond)
		if err != nil {
			slog.Errorf("%s notify pid:%d ag:%s err:%s", fun, pid, ag, err)
		}

		if string(res) != "OK" {
			slog.Errorf("%s notify pid:%d ag:%s res:%s", fun, pid, ag, res)
		}

	}

}

func (m *nodeMon) cbTwoway(a *paconn.Agent, btype int32, req []byte) (int32, []byte) {
	if string(req) == "GET JOBS" {
		return 0, m.allJobs()
	} else {
		return 0, nil
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
		time.Second*60*15,
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

}

func NewnodeMon() *nodeMon {
	nm := &nodeMon{
		monDisable: make(map[string]bool),
	}

	nm.Init()

	return nm
}

func (m *nodeMon) AddMonitor(monjob, monbin, monconf string) {
	// node monitor 没有放在jobmanager管理
	fun := "nodeMon.AddMonitor"

	if m.mon != nil {
		slog.Infof("%s been add", fun)
		return
	}

	if len(monjob) > 0 && len(monbin) > 0 && len(monconf) > 0 {
		// start node-monitor
		mc := &jobs.ManulConf{
			Name:        monbin,
			Args:        []string{monconf, m.agm.Listenport()},
			JobAuto:     true,
			BackOffCeil: time.Millisecond * 100,
		}

		m.mon = jobs.Newjob(monjob, mc, nil, nil)
		m.mon.Start()
	}

}

func (m *nodeMon) RemoveMonitor() {
	fun := "nodeMon.RemoveMonitor"
	slog.Infof("%s %v", fun, m.mon)
	if m.mon != nil {
		err := m.mon.Remove()
		slog.Infof("%s remove %s err:%v", fun, &m.mon, err)
		m.mon = nil
	}
}

var node_monitor *nodeMon = NewnodeMon()

func loadjob(tconf *sconf.TierConf, job string) (*jobs.ManulConf, error) {
	cmd, err := tconf.ToString(job, "cmd")
	if err != nil {
		return nil, err
	}

	args, err := tconf.ToSliceString(job, "args", " ")
	if err != nil {
		return nil, err
	}

	needjobkey := tconf.ToBoolWithDefault(job, "needjobkey", false)
	jobkey := tconf.ToStringWithDefault(job, "jobkey", "")
	stdlog := tconf.ToStringWithDefault(job, "stdlog", "")

	ruser := tconf.ToStringWithDefault(job, "user", "")

	auto := tconf.ToBoolWithDefault(job, "auto", true)

	backoffceil := tconf.ToIntWithDefault(job, "backoffceil", 20)

	m := &jobs.ManulConf{
		Name:        cmd,
		Args:        args,
		User:        ruser,
		Stdlog:      stdlog,
		NeedJobkey:  needjobkey,
		Jobkey:      jobkey,
		JobAuto:     auto,
		BackOffCeil: time.Second * time.Duration(backoffceil),
	}

	return m, nil
}

func reloadConf(conf string) error {
	fun := "engine.reloadConf"
	tconf := sconf.NewTierConf()
	err := tconf.LoadFromFile(conf)
	if err != nil {
		return err
	}

	printconf, err := tconf.StringCheck()
	if err != nil {
		return err
	}

	slog.Infof("%s conf:\n%s", fun, printconf)

	// load log config
	logdir := tconf.ToStringWithDefault("log", "dir", "")
	loglevel := tconf.ToStringWithDefault("log", "level", "TRACE")
	slog.Init(logdir, "node", loglevel)

	slog.Infof("%s conf:\n%s", fun, printconf)

	job_list := make([]string, 0)
	job_list, err = tconf.ToSliceString("node", "job_list", ",")
	if err != nil {
		slog.Warnf("%s job_list empty", fun)
	}

	nport, err := tconf.ToString("node", "port")
	if err != nil {
		slog.Warnf("%s nport empty", fun)
		return err
	}
	nodeRestPortFile = nport

	jobconfs := make(map[string]*jobs.ManulConf)
	for _, j := range job_list {
		mc, err := loadjob(tconf, j)
		if err != nil {
			return err
		}

		jobconfs[j] = mc

	}

	monjob := tconf.ToStringWithDefault("monitor", "jobname", "")
	monbin := tconf.ToStringWithDefault("monitor", "bin", "")
	monconf := tconf.ToStringWithDefault("monitor", "conf", "")
	mondisable := tconf.ToSliceStringWithDefault("monitor", "disable_list", ",", nil)

	node_monitor.upMonitorDisableList(mondisable)

	// 移除老的
	//  移除老的原因是，这里面没有做monitor的配置对比
	// 如果monitor的配置发生更新的话，不重启monitor怎么知道
	node_monitor.RemoveMonitor()
	node_monitor.AddMonitor(monjob, monbin, monconf)
	node_monitor.UpdateJobs(jobconfs)

	for _, j := range job_list {
		err = node_monitor.StartJob(j)
		if err != nil {
			slog.Errorf("%s start job:%s err:%s", fun, j, err)
		} else {
			slog.Infof("%s start job:%s ok", fun, j)
		}
	}

	return nil
}

var conffile string
var nodeRestPort string
var nodeRestPortFile string

func writePortfile() {
	fun := "engine.writePortfile"
	slog.Infof("%s write:%s port:%s", fun, nodeRestPortFile, nodeRestPort)
	err := sutil.WriteFile(nodeRestPortFile, []byte(fmt.Sprintf("%s\n", nodeRestPort)), 0600)
	if err != nil {
		slog.Errorf("%s write:%s port:%s err:%s", fun, nodeRestPortFile, nodeRestPort, err)
	}
}

func reload(w http.ResponseWriter, r *http.Request) {
	fun := "rest.reload"
	slog.Infof("%s %s", fun, r.URL.Path)

	err := reloadConf(conffile)
	if err != nil {
		slog.Fatalf("reload conf:%s err:%s", conffile, err)

		http.Error(w, err.Error(), 501)
		return

	} else {
		writePortfile()
	}

	fmt.Fprintf(w, "load:%s ok", conffile)

}

func Power(conf string) {
	fun := "engine.Power"
	conffile = conf
	err := reloadConf(conf)
	if err != nil {
		slog.Panicf("load conf:%s err:%s", conf, err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":")
	netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		slog.Panicf("StartHttp Listen: %s", err)
	}
	slog.Infof("%s listen:%s", fun, netListen.Addr())
	nodeRestPort = snetutil.IpAddrPort(netListen.Addr().String())

	writePortfile()

	http.HandleFunc("/conf/reload", reload)
	err = http.Serve(netListen, nil)
	if err != nil {
		slog.Panicf("HttpServ: %s", err)
	}

	//slog.Infoln("start http serv", restAddr)

	//pause := make(chan bool)
	// pause here
	//<- pause
	/*
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)

		// Block until a signal is received.
		for {
			s := <-c
			slog.Infoln("engine.Power Got signal:", s)
			err = reloadConf(conf)
			if err != nil {
				slog.Fatalf("reload conf:%s err:%s", conf, err)
			}
		}
	*/
}
