package jobs

import (
	"fmt"
	"sync"
	"sync/atomic"
	"errors"
	"time"


	"github.com/shawnfeng/sutil/slog"

)



type ManulConf struct {
	// 启动参数
	Name string
	Args []string

	// 是否自动控制
	JobAuto bool

	// 运行失败退避的最大值
	BackOffCeil time.Duration
}

func (m *ManulConf) String() string {
	return fmt.Sprintf("%d|%t|%s|%s", m.BackOffCeil/1000000, m.JobAuto, m.Name, m.Args)
}


type Job struct {
	// 运行job唯一标识
	id string

	mconfMu sync.Mutex
	mconf ManulConf

	userCmd chan *userCommand

	pid int32
	// 管理的进程停止的回调
	cbProcessStop func(*Job)
	// 管理的进程启动的回调
	cbProcessRun func(*Job)

	removeFlagMu sync.Mutex
	removeFlag bool
}

func Newjob(
	id string,
	conf *ManulConf,
	cbprocstop func(*Job),
	// 管理的进程启动的回调
	cbprocrun func(*Job),

) *Job {

	j := &Job {
		id: id,
		mconf: *conf,
		cbProcessStop: cbprocstop,
		cbProcessRun: cbprocrun,
		removeFlag: false,
		userCmd: make(chan *userCommand),
	}

	go j.live(j.userCmd)

	return j

}

func (m *Job) getConf() ManulConf {
	m.mconfMu.Lock()
	defer m.mconfMu.Unlock()
	return m.mconf
}

func (m *Job) String() string {
	conf := m.getConf()
	return fmt.Sprintf("%s|%s|%d", m.id, &conf, atomic.LoadInt32(&m.pid))

}

func (m *Job) Id() string {
	return m.id

}

func (m *Job) Pid() (int, error) {
	pid := atomic.LoadInt32(&m.pid) 
	if pid == 0 {
		return 0, errors.New("job not run")
	} else {
		return int(pid), nil
	}
}

func (m *Job) doUserCmd(c jobCmdType) error {
	fun := "Job.doUserCmd"
	slog.Infof("%s job:%s do cmd:%s", fun, m, c)

	m.removeFlagMu.Lock()
	defer m.removeFlagMu.Unlock()

	if m.removeFlag {
		return fmt.Errorf("job been remove")
	} else {
		if c == JOBCMD_REMOVE {
			m.removeFlag = true
		}
		m.userCmd <-&userCommand{cmd: c}
		return nil
	}

}

func (m *Job) Start() error {
	return m.doUserCmd(JOBCMD_START)
}


func (m *Job) Stop() error {
	return m.doUserCmd(JOBCMD_STOP)
}


func (m *Job) Kill() error {
	return m.doUserCmd(JOBCMD_KILL)

}

func (m *Job) Remove() error {
	return m.doUserCmd(JOBCMD_REMOVE)
}


func (m *Job) updateConf(conf *ManulConf) error {
	//fun := "Job.updateConf"
	func() {
		m.mconfMu.Lock()
		defer m.mconfMu.Unlock()
		m.mconf = *conf
	}()

	return m.doUserCmd(JOBCMD_UPCONF)

}



// 测试cmd.stdin 给sh执行的效果
type JobManager struct {
	jobsLock sync.Mutex
	jobs map[string]*Job

	cbProcessStop func(*Job)
	cbProcessRun func(*Job)

}

func NewJobManager(
	cbprocstop func(*Job),
	// 管理的进程启动的回调
	cbprocrun func(*Job),
) *JobManager {

	return &JobManager {
		jobs: make(map[string]*Job),

		cbProcessStop: cbprocstop,
		cbProcessRun: cbprocrun,

	}


}

func (m *JobManager) Runjobs() []string {
	fun := "JobManager.Runjobs"
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	pids := make([]string, 0)
	for _, j := range(m.jobs) {
		if p, err := j.Pid(); err == nil {

			pids = append(pids, fmt.Sprintf("%d", p))
		} else {
			slog.Warnf("%s job:%s pid err:%s", fun, j, err)
		}
	}

	return pids

}

func (m *JobManager) Start(jobid string) error {

	j, ok := func() (*Job, bool) {
		m.jobsLock.Lock()
		defer m.jobsLock.Unlock()
		j, ok := m.jobs[jobid]
		return j, ok
	}()

	if ok {
		return j.Start()
	} else {
		return errors.New("jobid not found")
	}

}

func (m *JobManager) Update(confs map[string]*ManulConf) {
	fun := "JobManager.Update"
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	for k, v := range(confs) {
		j, ok := m.jobs[k]
		if ok {
			slog.Infof("%s update job:%s conf:%s", fun, k, v)
			err := j.updateConf(v)
			if err != nil {
				slog.Errorf("%s update job:%s conf:%s err:%s", fun, k, v, err)
			}

		} else {
			slog.Infof("%s new job:%s conf:%s", fun, k, v)
			m.jobs[k] = Newjob(k, v, m.cbProcessStop, m.cbProcessRun)
		}

	}

	for k, v := range m.jobs {
		_, ok := confs[k]
		if !ok {
			slog.Warnf("%s del job:%s", fun, k)
			delete(m.jobs, k)
			err := v.Remove()
			if err != nil {
				slog.Errorf("%s remove job:%s conf:%s err:%s", fun, k, v, err)
			}

		}
	}

}
