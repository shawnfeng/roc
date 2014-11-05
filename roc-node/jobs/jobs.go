package jobs

import (
	"os/exec"
	"fmt"
	"errors"
	"time"
	"bufio"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"

)

type jobLoopType int32

const (
	Loop_STOP    jobLoopType = 0

	Loop_RUN    jobLoopType = 1
	Loop_RUNCHEACK    jobLoopType = 2
	Loop_BACKOFF    jobLoopType = 3

)

func (m jobLoopType) String() string {
	s := "UNKNOWN"

	if Loop_STOP == m {
		s = "STOP"

	} else if Loop_RUN == m {
		s = "RUN"

	} else if Loop_RUNCHEACK  == m {
		s = "RUNCHEACK"

	} else if Loop_BACKOFF == m {
		s = "BACKOFF"

	}


	return s
}



type jobExitType int32

const (
	Exit_OK                 jobExitType = 0
	Exit_Err_StdoutPipe     jobExitType = 1
	Exit_Err_CmdStart       jobExitType = 2

	Exit_Err_Kill           jobExitType = 3
	Exit_Err_JobOtherErr    jobExitType = 4

)




func (m jobExitType) String() string {
	s := "UNKNOWN"

	if Exit_OK == m {
		s = "OK"

	} else if Exit_Err_StdoutPipe == m {
		s = "Err_StdoutPipe"

	} else if Exit_Err_CmdStart  == m {
		s = "Err_CmdStart"

	} else if Exit_Err_Kill == m {
		s = "Err_Kill"

	} else if Exit_Err_JobOtherErr == m {
		s = "Err_JobOtherErr"

	}


	return s
}


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



	// exec.Command 的返回
	cmd *exec.Cmd

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
	//m.cmd.Process.Pid
	//Loop_RUN

	if m.loopState != Loop_RUN {
		return 0, errors.New("job not run")
	}

	if m.cmd != nil && m.cmd.Process != nil {
		return m.cmd.Process.Pid, nil
	} else {
		return 0, errors.New("process not run")
	}

}



func (m *Job) Kill() error {
	fun := "Job.Kill"

	slog.Warnf("%s %s", fun, m)
	if m.loopState == Loop_STOP {
		return errors.New("job loop not run")
	}

	if m.cmd != nil && m.cmd.Process != nil {
		return m.cmd.Process.Kill()
	} else {
		return errors.New("process not run")
	}
}

func (m *Job) Start() error {
	if m.loopState != Loop_STOP {
		return errors.New(fmt.Sprintf("job start err loop state %d", m.loopState))
	} else {
		go m.loop()
		return nil
	}

}

func (m *Job) loopStateChange(logkey string, newstate jobLoopType) {
	oldstate := m.loopState
	if oldstate == newstate {
		slog.Warnf("loopStateChange %s new eq old %s", logkey, oldstate)
	} else {
		m.loopState = newstate
		slog.Infof("loopStateChange %s %s:%s", logkey, oldstate, m.loopState)
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

	m.stampBegin = time.Now().Unix()

	m.cmd = exec.Command(m.mconf.Name, m.mconf.Args...)
	stdout, err := m.cmd.StdoutPipe()
	if err != nil {
		return Exit_Err_StdoutPipe, err
	}


	if err := m.cmd.Start(); err != nil {
		return Exit_Err_CmdStart, err
	}


	slog.Infof("%s %s start PID:%d", fun, m, m.cmd.Process.Pid)

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
	if err := m.cmd.Wait(); err != nil {
		return Exit_Err_JobOtherErr, err
	}


	return Exit_OK, nil
}


// 测试cmd.stdin 给sh执行的效果
type JobManager struct {
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
	if j, ok := m.jobs[jobid]; ok {
		return j.Start()
	} else {
		return errors.New("jobid not found")
	}

}

func (m *JobManager) Update(confs map[string]*ManulConf) {

	for k, v := range(confs) {
		j, ok := m.jobs[k]
		if ok {
			j.updateConf(v)
		} else {
			m.jobs[k] = Newjob(v, m.cbProcessStop, m.cbProcessRun)
		}

	}

}
