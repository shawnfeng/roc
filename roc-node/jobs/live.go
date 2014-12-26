package jobs

import (
	"os"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"bufio"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
)
// TODO
// backoff第一次是立即过来的，这个地方stopbackoff的行为有点不符合啊！！改

// 当前job状态加个记录
// pid 要不要直接在run里面设置？

// backoff期间收到kill指令？
// 没启动完收到kill
// backoff期间收到stop？
// start 时候应该停止backoff
// 实际是只要收到用户的指令就要停止backoff过程

func (m *Job) killProc() (erro error) {
	fun := "Job.killProc"
	slog.Infof("%s killproc job:%s", fun, m)
	pid := atomic.LoadInt32(&m.pid)
	if pid == 0 {
		slog.Warnf("%s killproc %s pid 0", fun, m)
		return fmt.Errorf("proc not run")
	} else {
		p, err := os.FindProcess(int(pid))
		if err != nil {
			slog.Warnf("%s killproc jobs:%s find process err:%s", fun, m, err)
			return err
		} else {
			err = p.Kill()
			if err != nil {
				slog.Warnf("%s killproc job:%s kill err:%s", fun, m, err)
				return err
			}
		}
	}

	return nil

}


func (m *Job) doUserCtrl(c jobRunCtrl) error {
	fun := "Job.doUserCtrl"
	slog.Infof("%s doctrl job:%s ctrl:%s", fun, m, c)

	m.runCtrlMu.Lock()
	defer m.runCtrlMu.Unlock()


	if m.runCtrl == RUNCTRL_REMOVE {
		ers := fmt.Sprintf("been removed job:%s", m)
		slog.Errorf("%s %s", fun, ers)
		return fmt.Errorf(ers)
	}

	m.runBackOff.Reset()
	m.stopBackOff.Reset()
	if c == RUNCTRL_START {
		m.runCtrl = RUNCTRL_START

	} else if c == RUNCTRL_KILL {
		return m.killProc()

	} else if c == RUNCTRL_STOP {
		m.runCtrl = RUNCTRL_STOP
		m.killProc()
	
	} else if c == RUNCTRL_REMOVE {
		m.runCtrl = RUNCTRL_REMOVE
		m.killProc()
	} else {
		slog.Warnf("%s unknown ctrl job:%s do cmd:%s", fun, m, c)

		return fmt.Errorf("unknown ctrl job")
	}

	return nil


}

func (m *Job) updateConf(conf *ManulConf) error {
	fun := "Job.updateConf"
	m.runCtrlMu.Lock()
	defer m.runCtrlMu.Unlock()
	slog.Infof("%s job:%s old:%s new:%s", fun, m, &m.mconf, conf)

	m.mconf = *conf
	m.runBackOff.Reset()
	m.stopBackOff.Reset()
	return nil
}



func (m *Job) Start() error {
	return m.doUserCtrl(RUNCTRL_START)
}


func (m *Job) Stop() error {
	return m.doUserCtrl(RUNCTRL_STOP)
}


func (m *Job) Kill() error {
	return m.doUserCtrl(RUNCTRL_KILL)

}

func (m *Job) Remove() error {
	return m.doUserCtrl(RUNCTRL_REMOVE)
}



func (m *Job) live() {
	fun := "Job.live"
	for {
		m.runCtrlMu.Lock()
		if m.runCtrl == RUNCTRL_REMOVE {
			// LOG
			slog.Infof("%s remove job:%s %s", fun, m, m.runCtrl)
			m.runCtrlMu.Unlock()
			return

		} else if m.runCtrl == RUNCTRL_STOP {
			slog.Infof("%s stop state backoff job:%s %s", fun, m, m.runCtrl)
			m.runCtrlMu.Unlock()
			m.stopBackOff.BackOff()
			m.runCtrlMu.Lock()
			slog.Infof("%s stop backoff over job:%s %s", fun, m, m.runCtrl)
			m.runCtrlMu.Unlock()
		} else if m.runCtrl == RUNCTRL_START {
			// very ugly,but what can i do?
			// in liveRun to unlock
			stat := stime.NewTimeStat()
			err := m.run(&m.runCtrlMu, m.mconf.Name, m.mconf.Args...)
			if m.cbProcessStop != nil {
				go m.cbProcessStop(atomic.LoadInt32(&m.pid), m)
			}
			runDuration := stat.Duration()

			m.runCtrlMu.Lock()
			atomic.StoreInt32(&m.pid, 0)
			slog.Infof("%s check config job:%s conf:%s ctrl:%s", fun, m, &m.mconf, m.runCtrl)
			if !m.mconf.JobAuto {
				// 用户设置了非自动控制，不再启动
				m.runCtrl = RUNCTRL_STOP
				slog.Infof("%s not auto set job:%s %s", fun, m, RUNCTRL_STOP)
			}
			m.runCtrlMu.Unlock()

			if err == nil {
				slog.Infof("%s exitok call backoffRun job:%s", fun, m)
				m.runBackOff.Reset()
			} else {
				if runDuration < 60*1000*1000*1000 {
					slog.Infof("%s exit short call backoffRun job:%s err:%s", fun, m, err)
					m.runBackOff.BackOff()
				} else {
					slog.Infof("%s exit long call backoffRun job:%s err:%s", fun, m, err)
					m.runBackOff.Reset()
				}
			}


		} else {
			slog.Fatalf("%s unknown ctrl job:%s do:%s", fun, m, m.runCtrl)
			m.runCtrlMu.Unlock()
			m.stopBackOff.BackOff()
		}

	}
}


func (m *Job) run(mu *sync.Mutex, cmdname string, cmdargs ...string) error {
	fun := "Job.run"

	slog.Infof("%s exec cmd job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
	cmd := exec.Command(cmdname, cmdargs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		slog.Errorf("%s over done stdio job:%s err:%s", fun, m, err)
		mu.Unlock()
		return err
	}


	slog.Infof("%s start cmd job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
	if err := cmd.Start(); err != nil {
		slog.Errorf("%s over done cmdstart job:%s err:%s", fun, m, err)
		mu.Unlock()
		return err
	}
	atomic.StoreInt32(&m.pid, int32(cmd.Process.Pid))
	slog.Infof("%s jos start %s", fun, m)

	mu.Unlock()


	if m.cbProcessRun != nil {
		go m.cbProcessRun(atomic.LoadInt32(&m.pid), m)
	}

	// StdoutPipe returns a pipe that will be connected to the command's standard output when the command starts.
	// Wait will close the pipe after seeing the command exit,
	// so most callers need not close the pipe themselves;
	// however, an implication is that it is incorrect to call Wait before all reads from the pipe have completed.
	// For the same reason, it is incorrect to call Run when using StdoutPipe. See the example for idiomatic usage.

	slog.Infof("%s readstdout job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
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


	slog.Infof("%s wait job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
	// 只要返回status不是0就会有err
	if err := cmd.Wait(); err != nil {
		slog.Warnf("%s exit job:%s err:%s", fun, m, err)
		return err
	}

	slog.Infof("%s exit ok job:%s", fun, m)
	return nil
}

