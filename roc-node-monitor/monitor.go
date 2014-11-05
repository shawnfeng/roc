package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"strconv"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/paconn"
)



type Monitor struct {
	nodePort string
	nodeAgent *paconn.Agent
	agentFail chan error
	jobs []int

}

func NewMonitor(port string) *Monitor {
	m := &Monitor {
		nodePort: port,
		nodeAgent: nil,
		agentFail: make(chan error),
		jobs: make([]int, 0),
	}


	return m
}

func (m *Monitor) agentClose(a *paconn.Agent, data []byte, err error) {
	fun := "angentClose"
	slog.Infof("%s %s %v %s %s", fun, a, data, data, err)

	// 父进程应该已经崩溃，马上进行进程排查
	m.checkMe()
	m.agentFail <-err
}


func (m *Monitor) agentTwoway(a *paconn.Agent, res []byte) []byte {
	fun := "Monitor.agentTwoway"

	slog.Infof("%s jobs:%s", fun, res)

	m.parseJobs(res)
	return []byte("OK")

}

func (m *Monitor) parseJobs(sjobs []byte) {
	fun := "Monitor.parseJobs"
	jobs := make([]string, 0)
	if len(sjobs) > 0 {
		jobs = strings.Split(string(sjobs), ",")
	} else {
		slog.Infof("%s empty jobs", fun)
	}
	pids := make([]int, 0)
	for _, j := range(jobs) {
		pid, err := strconv.Atoi(j)
		if err != nil {
			slog.Errorf("%s getpid job:%s err:%s", fun, j, err)
		} else {
			pids = append(pids, pid)
		}
	}

	m.jobs = pids

}

func (m *Monitor) syncJobs() {
	fun := "Monitor.syncJobs"
	if m.nodeAgent != nil {
		res, err := m.nodeAgent.Twoway([]byte("GET JOBS"), 200)
		if err != nil {
			slog.Errorf("%s getjobs err:%s", fun, err)
		} else {
			slog.Infof("%s jobs:%s", fun, res)
			m.parseJobs(res)

		}
	} else {
		slog.Errorf("%s agent nil", fun)
	}

}



func (m *Monitor) checkMe() {
	fun := "Monitor.checkMe"
	ppid := os.Getppid()
	slog.Tracef("%s ppid:%d", fun, ppid)
	if ppid == 1 {
		slog.Warnf("%s clear ppid:%d jobs:%s", fun, ppid, m.jobs)
		for _, j := range(m.jobs) {
			// 对不存在的pid FindProcess，不会出错
			// 但是调用kill时候，会提示错误：no such process
			p, err := os.FindProcess(j)
			if err != nil {
				slog.Errorf("%s FindProcess:%d err:s", fun, j, err)
			} else {
				slog.Infof("%s kill:%d", fun ,j)
				err = p.Kill()
				if err != nil {
					slog.Errorf("%s Kill:%d err:s", fun, j, err)
				}
			}

		}

		// 清理完后，自己OVER
		slog.Infof("%s BYE BYE", fun)
		//time.Sleep(time.Second * 10)
		os.Exit(0)
	}

}

func (m *Monitor) cronCommonJobs() {
	ticker0 := time.NewTicker(time.Second * time.Duration(10))
	ticker1 := time.NewTicker(time.Millisecond * time.Duration(500))

	for {
		select {
		case <-ticker0.C:
			m.syncJobs()
		case <-ticker1.C:
			m.checkMe()
		}
	}


}


// 保证agent的成活
func (m *Monitor) cronLive() {
	fun := "Monitor.cronLive"

	for {
		ag, err := paconn.NewAgentFromAddr(
			fmt.Sprintf("127.0.0.1:%s", m.nodePort),
			time.Second*60*15,
			0,
			nil,
			m.agentTwoway,
			m.agentClose,
		)

		if err != nil {
			slog.Errorf("%s Dial err:%s", fun, err)
		} else {
			m.nodeAgent = ag
			m.syncJobs()

			failerr := <-m.agentFail
			slog.Errorf("%s Agent failerr:%s", fun, failerr)
		}
		time.Sleep(time.Millisecond * 500)

	}


}



func main() {
	slog.Init("./log", "node-monitor", "DEBUG")
	// node 开启的端口
	nodePort := os.Args[1]

	m := NewMonitor(nodePort)

	go m.cronLive()
	m.cronCommonJobs()
}


