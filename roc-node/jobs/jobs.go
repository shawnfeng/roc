package jobs

import (
	"fmt"
	"sync"
	"sync/atomic"
	"errors"
	"time"

	"github.com/shawnfeng/sutil/stime"
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
	pid int32
	// 管理的进程停止的回调
	cbProcessStop func(int32, *Job)
	// 管理的进程启动的回调
	cbProcessRun func(int32, *Job)

	// job运行失败重启退避
	runBackOff *stime.BackOffCtrl
	// job没有运行等待退避
	stopBackOff *stime.BackOffCtrl


	runCtrlMu sync.Mutex
	runCtrl jobRunCtrl
	mconf ManulConf

}

func Newjob(
	id string,
	conf *ManulConf,
	cbprocstop func(int32, *Job),
	// 管理的进程启动的回调
	cbprocrun func(int32, *Job),

) *Job {

	j := &Job {
		id: id,

		cbProcessStop: cbprocstop,
		cbProcessRun: cbprocrun,

		runBackOff: stime.NewBackOffCtrl(time.Millisecond * 100, conf.BackOffCeil),
		// 固定10s
		stopBackOff: stime.NewBackOffCtrl(time.Second * 10, time.Second * 10),

		runCtrl: RUNCTRL_STOP,
		mconf: *conf,
	}

	go j.live()

	return j

}


func (m *Job) String() string {
	return fmt.Sprintf("%s|%d", m.id, atomic.LoadInt32(&m.pid))

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


// 测试cmd.stdin 给sh执行的效果
type JobManager struct {
	jobsLock sync.Mutex
	jobs map[string]*Job

	cbProcessStop func(int32, *Job)
	cbProcessRun func(int32, *Job)

}

func NewJobManager(
	cbprocstop func(int32, *Job),
	// 管理的进程启动的回调
	cbprocrun func(int32, *Job),
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
