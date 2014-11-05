package jobs

import (
	"os/exec"
	"fmt"
	"errors"
	"time"

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
	id string
	// 启动参数
	name string
	args []string

	// 是否自动控制
	jobAuto bool

	// 运行失败退避的最大值
	backOffCeil time.Duration


}

func (m *ManulConf) String() string {
	return fmt.Sprintf("%s@%d@%s%s%t", m.id, m.backOffCeil/1000000, m.name, m.args, m.jobAuto)

}


type job struct {
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
}


//func Newjob(id string, name string, args []string, backoff int64) *job {
func Newjob(conf *ManulConf) *job {

	j := &job {
		mconf: conf,

		stampBegin: 0,
		backOff: stime.NewBackOffCtrl(time.Second * 1, conf.backOffCeil),

		loopState: Loop_STOP,

	}


	return j

}


func (m *job) String() string {
	return fmt.Sprintf("%s@%s", m.mconf, m.loopState)

}

func (m *job) updateConf(conf *ManulConf) bool {
	fun := "job.updateConf"
	slog.Infof("%s %s:%s", fun, m.mconf, conf)
	isup := false

	if conf.id != m.mconf.id {
		m.mconf.id = conf.id
		isup = true
	}


	if conf.name != m.mconf.name {
		m.mconf.name = conf.name
		isup = true
	}

	if conf.jobAuto != m.mconf.jobAuto {
		m.mconf.jobAuto = conf.jobAuto
		isup = true
	}

	if conf.backOffCeil != m.mconf.backOffCeil {
		m.mconf.backOffCeil = conf.backOffCeil
		isup = true
	}

	if len(conf.args) != len(m.mconf.args) {
		m.mconf.args = conf.args
		isup = true
	} else {

		for i := 0; i < len(conf.args); i++ {
			if conf.args[i] != m.mconf.args[i] {
				m.mconf.args = conf.args
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

func (m *job) kill() error {
	fun := "job.kill"

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

func (m *job) start() error {
	if m.loopState != Loop_STOP {
		return errors.New(fmt.Sprintf("job start err loop state %d", m.loopState))
	} else {
		go m.loop()
		return nil
	}

}

func (m *job) loopStateChange(logkey string, newstate jobLoopType) {
	oldstate := m.loopState
	if oldstate == newstate {
		slog.Warnf("loopStateChange %s new eq old %s", logkey, oldstate)
	} else {
		m.loopState = newstate
		slog.Infof("loopStateChange %s %s:%s", logkey, oldstate, m.loopState)
	}


}

func (m *job) loop() {
	fun := "job.loop"
	is1stRun := true


	defer func() {
		m.loopStateChange(fun, Loop_STOP)
	}()

	for {

		if !is1stRun && !m.mconf.jobAuto {
			// 如果不是第一次运行，并且不是自动管理
			// 此时不主动启动
			break
		}
		is1stRun = false


		m.loopStateChange(fun, Loop_RUN)
		bgtime := time.Now().UnixNano()/1000
		rtype, err := m.run()
		slog.Infof("%s %s runtime:%d", fun, m, time.Now().UnixNano()/1000-bgtime)
		m.loopStateChange(fun, Loop_RUNCHEACK)

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

		if !m.mconf.jobAuto {
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

func (m *job) run() (jobExitType, error) {
	fun := "job.run"

	m.stampBegin = time.Now().Unix()

	m.cmd = exec.Command(m.mconf.name, m.mconf.args...)
	stdout, err := m.cmd.StdoutPipe()
	if err != nil {
		return Exit_Err_StdoutPipe, err
	}


	if err := m.cmd.Start(); err != nil {
		return Exit_Err_CmdStart, err
	}


	slog.Infof("%s %s start PID:%d", fun, m, m.cmd.Process.Pid)




	buffer := make([]byte, 4096)
	// StdoutPipe returns a pipe that will be connected to the command's standard output when the command starts.
	// Wait will close the pipe after seeing the command exit,
	// so most callers need not close the pipe themselves;
	// however, an implication is that it is incorrect to call Wait before all reads from the pipe have completed.
	// For the same reason, it is incorrect to call Run when using StdoutPipe. See the example for idiomatic usage.
	for {
		bytesRead, err := stdout.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" {
				slog.Warnf("%s %s stdout read err:%s", fun, m, err)
			} else {
				slog.Infof("%s %s >:EOF", fun, m)
			}
			break
		}
		// 不考虑截断log输出的问题
		slog.Infof("%s %s >:%s", fun, m, buffer[:bytesRead])
		//time.Sleep(time.Second * 5)
	}


	// 只要返回status不是0就会有err
	if err := m.cmd.Wait(); err != nil {
		return Exit_Err_JobOtherErr, err
	}


	return Exit_OK, nil
}


// 测试cmd.stdin 给sh执行的效果

type JobManager interface {
	//Add(name string, arg ...string) string
	//Del(id string)
	//Restart(id string)
	//Stop(id string)
	//Start(id string)
	Update(confs map[string]*ManulConf)
}


type jobMan struct {
	jobs map[string]*job

}

func (m *jobMan) Update(confs map[string]*ManulConf) {

	for k, v := range(confs) {
		j, ok := m.jobs[k]
		if ok {
			j.updateConf(v)
		} else {
			m.jobs[k] = Newjob(v)
		}

	}

}
