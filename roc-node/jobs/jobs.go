package jobs

import (
	"os/exec"
	"os"
	"fmt"
	"sync"
	"sync/atomic"
	"errors"
	"time"
	"bufio"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"

)



type ManulConf struct {
	// 运行job唯一标识
	Id string
	// 启动参数
	Name string
	Args []string

	// 是否自动控制
	JobAuto bool

	// 运行失败退避的最大值
	BackOffCeil time.Duration


}

func (m *ManulConf) String() string {
	return fmt.Sprintf("%s@%d@%s%s%t", m.Id, m.BackOffCeil/1000000, m.Name, m.Args, m.JobAuto)

}


type Job struct {
	mconf *ManulConf

	// 运行开始时间
	stampBegin int64


	// 启动失败定义：本次启动距离上次启动<60s，且不是因为接口控制的重启。成功一次计数清0,正常退出不算失败
	// 程序返回不是0
	// 内部错误也计入失败
	backOff *stime.BackOffCtrl


	// 是否在处于loop run的哪个状态
	loopState jobLoopType



	pid int32

	// 管理的进程停止的回调
	cbProcessStop func(*Job)
	// 管理的进程启动的回调
	cbProcessRun func(*Job)
}


//func Newjob(id string, name string, args []string, backoff int64) *job {
func Newjob(
	conf *ManulConf,
	cbprocstop func(*Job),
	// 管理的进程启动的回调
	cbprocrun func(*Job),

) *Job {

	j := &Job {
		mconf: conf,

		stampBegin: 0,
		backOff: stime.NewBackOffCtrl(time.Millisecond * 100, conf.BackOffCeil),

		loopState: Loop_STOP,

		cbProcessStop: cbprocstop,
	// 管理的进程启动的回调
		cbProcessRun: cbprocrun,


	}


	return j

}


func (m *Job) String() string {
	return fmt.Sprintf("%s@%s", m.mconf, m.loopState)

}

func (m *Job) Id() string {
	return m.mconf.Id

}

func (m *Job) AutoChange(isauto bool) {
	m.mconf.JobAuto = isauto
}

func (m *Job) updateConf(conf *ManulConf) bool {
	fun := "Job.updateConf"
	slog.Infof("%s %s:%s", fun, m.mconf, conf)
	isup := false

	if conf.Id != m.mconf.Id {
		m.mconf.Id = conf.Id
		isup = true
	}


	if conf.Name != m.mconf.Name {
		m.mconf.Name = conf.Name
		isup = true
	}

	if conf.JobAuto != m.mconf.JobAuto {
		m.mconf.JobAuto = conf.JobAuto
		isup = true
	}

	if conf.BackOffCeil != m.mconf.BackOffCeil {
		m.mconf.BackOffCeil = conf.BackOffCeil
		isup = true
	}

	if len(conf.Args) != len(m.mconf.Args) {
		m.mconf.Args = conf.Args
		isup = true
	} else {

		for i := 0; i < len(conf.Args); i++ {
			if conf.Args[i] != m.mconf.Args[i] {
				m.mconf.Args = conf.Args
				isup = true
				break
			}
		}

	}

	if isup {
		// 只要有更新就reset backoff
		m.backOff.Reset()

	}

	return isup

}

func (m *Job) Pid() (int, error) {
	pid := atomic.LoadInt32(&m.pid) 
	if pid == 0 {
		return 0, errors.New("job not run")
	} else {
		return int(pid), nil
	}
}



func (m *Job) Kill() error {
	fun := "Job.Kill"

	slog.Warnf("%s %s", fun, m)

	pid := atomic.LoadInt32(&m.pid) 
	if pid == 0 {
		slog.Warnf("%s %s state not run", fun, m)
		return errors.New("job loop not run")
	} else {
		p, err := os.FindProcess(int(pid))
		if err != nil {
			slog.Warnf("%s %s find process err:%s", fun, m, err)
			return err
		}
		err = p.Kill()
		if err != nil {
			slog.Warnf("%s %s kill err:%s", fun, m, err)
			return err
		}

		return nil

	}

}

func (m *Job) Start() error {
	if m.loopState != Loop_STOP {
		return fmt.Errorf("job start err loop state %s", m.loopState)
	} else {
		go m.loop()
		return nil
	}

}

func (m *Job) loopStateChange(logkey string, newstate jobLoopType) {
	fun := "Job.loopStateChange"
	oldstate := m.loopState
	if oldstate == newstate {
		slog.Warnf("%s %s %s new eq old %s", fun, m, logkey, oldstate)
	} else {
		m.loopState = newstate
		slog.Infof("%s %s %s %s:%s", fun, m, logkey, oldstate, m.loopState)
	}


}

func (m *Job) loop() {
	fun := "Job.loop"
	is1stRun := true


	defer func() {
		m.loopStateChange(fun, Loop_STOP)
	}()

	for {

		if !is1stRun && !m.mconf.JobAuto {
			// 不是自主启动的程序，退出时候，不再自动启动
			break
		}
		is1stRun = false


		m.loopStateChange(fun, Loop_RUN)
		bgtime := time.Now().UnixNano()/1000
		rtype, err := m.run()
		slog.Infof("%s %s runtime:%d", fun, m, time.Now().UnixNano()/1000-bgtime)
		m.loopStateChange(fun, Loop_RUNCHEACK)
		if m.cbProcessStop != nil {
			m.cbProcessStop(m)
		}

		if err != nil {
			slog.Warnf("%s %s rtype:%s err:%s", fun, m, rtype, err)
		} else {
			slog.Infof("%s %s rtype:%s", fun, m, rtype)
		}

		// --------------------------
		if rtype == Exit_OK {
			// 返回正常，程序退出后直接重新启动
			m.backOff.Reset()
			continue
		}

		if !m.mconf.JobAuto {
			// 如果不是自动管理的，就不退避了
			break
		}


		intv := time.Now().Unix() - m.stampBegin
		// 60s 就异常退出了，就算失败
		if intv  < 60 {
			m.loopStateChange(fun, Loop_BACKOFF)
			m.backOff.BackOff()
		} else {
			// 如果运行的时间达到了指定的间隔了，则重新计算退避值
			m.backOff.Reset()
		}

	}

}

func (m *Job) run() (jobExitType, error) {
	fun := "Job.run"
	defer send exittype chan
	m.stampBegin = time.Now().Unix()

	cmd := exec.Command(m.mconf.Name, m.mconf.Args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Exit_Err_StdoutPipe, err
	}


	if err := cmd.Start(); err != nil {
		return Exit_Err_CmdStart, err
	}



	slog.Infof("%s %s start PID:%d", fun, m, cmd.Process.Pid)

	atomic.StoreInt32(&m.pid, int32(cmd.Process.Pid))
	defer atomic.StoreInt32(&m.pid, 0)

	if m.cbProcessRun != nil {
		m.cbProcessRun(m)
	}

	// StdoutPipe returns a pipe that will be connected to the command's standard output when the command starts.
	// Wait will close the pipe after seeing the command exit,
	// so most callers need not close the pipe themselves;
	// however, an implication is that it is incorrect to call Wait before all reads from the pipe have completed.
	// For the same reason, it is incorrect to call Run when using StdoutPipe. See the example for idiomatic usage.
	stdbuff := bufio.NewReader(stdout)
	for {
		// log 按行输出，读取不到换行符号时候，会阻塞在这里哦
		logline, err := stdbuff.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				slog.Warnf("%s %s stdout read err:%s", fun, m, err)
			} else {
				slog.Infof("%s %s >:EOF", fun, m)
			}
			break
		}
		slog.Infof("%s %s >:%s", fun, m, logline)
		//time.Sleep(time.Second * 5)
	}


	// 只要返回status不是0就会有err
	if err := cmd.Wait(); err != nil {
		return Exit_Err_JobOtherErr, err
	}


	return Exit_OK, nil
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
			isup := j.updateConf(v)
			slog.Infof("%s update job:%s conf:%s isup:%t", fun, k, v, isup)
		} else {
			slog.Infof("%s new job:%s conf:%s", fun, k, v)
			m.jobs[k] = Newjob(v, m.cbProcessStop, m.cbProcessRun)
		}

	}

	for k, v := range m.jobs {
		_, ok := confs[k]
		if !ok {
			slog.Warnf("%s del job:%s", fun, k)
			delete(m.jobs, k)
			v.AutoChange(false)
			v.Kill()
		}
	}

}
