// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package jobs

import (
	"syscall"
	"os"
	"io"
	"os/user"
	"fmt"
	"time"
	"strings"
	"os/exec"
	"sync"
	"sync/atomic"
	"bufio"
	"strconv"

	"github.com/shawnfeng/sutil"
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

			jobkeyargs := make([]string, 0)
			if m.mconf.NeedJobkey {
				if len(m.mconf.Jobkey) > 0 {
					m.jobKey = m.mconf.Jobkey
				}
				if len(m.jobKey) == 0 {
					m.jobKey = sutil.GetUUID()
				}

				jobkeyargs = append(jobkeyargs, "--skey")
				jobkeyargs = append(jobkeyargs, m.jobKey)
			}

			err := m.run(&m.runCtrlMu, m.mconf.User, m.mconf.Stdlog, m.mconf.Name, append(m.mconf.Args, jobkeyargs...)...)


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
			/*
			if err == nil {
				slog.Infof("%s exitok call backoffRun job:%s", fun, m)
				m.runBackOff.Reset()
			} else {
				if runDuration < 60*time.Second {
					slog.Infof("%s exit short call backoffRun job:%s err:%s", fun, m, err)
					m.runBackOff.BackOff()
				} else {
					slog.Infof("%s exit long call backoffRun job:%s err:%s", fun, m, err)
					m.runBackOff.Reset()
				}
			}
            */
			// 修改为对成功和失败的都执行退避
			if runDuration < 60*time.Second {
				slog.Infof("%s exit short call backoffRun job:%s err:%v", fun, m, err)
				m.runBackOff.BackOff()
			} else {
				slog.Infof("%s exit long call backoffRun job:%s err:%v", fun, m, err)
				m.runBackOff.Reset()
			}



		} else {
			slog.Fatalf("%s unknown ctrl job:%s do:%s", fun, m, m.runCtrl)
			m.runCtrlMu.Unlock()
			m.stopBackOff.BackOff()
		}

	}
}

func (m *Job) setRunuser(cmd *exec.Cmd, ruser string) {
	fun := "Job.setRunuser"
	if len(ruser) == 0 {
		return
	}
	// 检查当前用户是否是root，不是root不支持
	u, err := user.Current()
	if err != nil {
		slog.Warnf("%s user:%s current check err:%s", fun, ruser, err)
		return
	}

	if u.Uid != "0" {
		slog.Warnf("%s user:%s current user not root %s", fun, ruser, u)
		return
	}

	u, err = user.Lookup(ruser)
	if err != nil {
		slog.Warnf("%s user:%s not found err:%s", fun, ruser, err)
		return
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		slog.Warnf("%s user:%s uinfo:%s uid not int err:%s", fun, ruser, u, err)
		return
	}

	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		slog.Warnf("%s user:%s uinfo:%s gid not int err:%s", fun, ruser, u, err)
		return
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{}
	slog.Infof("%s user:%s info:%s", fun, ruser, u)
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}


}

func (m *Job) makeStdReadChan(r io.Reader) (chan string) {
	fun := "Job.makeStdReadChan"
    read := make(chan string)
	go func() {
		stdbuff := bufio.NewReader(r)
		for {
			line, err := stdbuff.ReadString('\n')
			if err != nil {
				close(read)
				slog.Warnf("%s %s read err:%s", fun, m, err)
				return
			} else {
				read <- line
			}
		}

	}()
	return read

}

func (m *Job) run(mu *sync.Mutex, ruser, stdlog string, cmdname string, cmdargs ...string) error {
	fun := "Job.run"


	var stdlogw *bufio.Writer
	if len(stdlog) > 0 {
		var stdlogf *os.File
		var err error
		pos := strings.LastIndex(stdlog, "/")
		if pos != -1 {
			err = os.MkdirAll(stdlog[:pos], 0777)
		}

		if err != nil {
			slog.Errorf("%s mkdir:%s err:%s", fun, stdlog, err)

		} else {
			stdlogf, err = os.OpenFile(stdlog, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
			if err != nil {
				slog.Errorf("%s open file:%s err:%s", fun, stdlog, err)
			} else {
				defer stdlogf.Close()
				stdlogw = bufio.NewWriter(stdlogf)
			}
		}

	}


	// =================================
	slog.Infof("%s exec cmd job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
	cmd := exec.Command(cmdname, cmdargs...)

	m.setRunuser(cmd, ruser)


	stdout, err := cmd.StdoutPipe()
	if err != nil {
		slog.Errorf("%s over done stdout job:%s err:%s", fun, m, err)
		mu.Unlock()
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		slog.Errorf("%s over done stderr job:%s err:%s", fun, m, err)
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

	stdoutchan := m.makeStdReadChan(stdout)
	stderrchan := m.makeStdReadChan(stderr)

	for {
		var ln string
		var ok bool
		select {
		case ln, ok = <-stdoutchan:
			if !ok {
				stdoutchan = nil
			}
		case ln, ok = <-stderrchan:
			if !ok {
				stderrchan = nil
			}
		}

		if stdoutchan == nil &&
			stderrchan == nil {
			break
		}

		if stdlogw != nil {
			stdlogw.WriteString(ln)
			stdlogw.Flush()

		} else {
			slog.Infof("%s STDE %s >:%s", fun, m, ln)
		}

	}

	/*

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
		if stdlogw != nil {
			stdlogw.WriteString(logline)
			stdlogw.Flush()

		} else {
			slog.Infof("%s STDO %s >:%s", fun, m, logline)
		}
	}

	stder := bufio.NewReader(stderr)
	for {
		// log 按行输出，读取不到换行符号时候，会阻塞在这里哦
		logline, err := stder.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				slog.Warnf("%s %s stderr read err:%s", fun, m, err)
			} else {
				slog.Infof("%s %s >:EOF", fun, m)
			}
			break
		}
		if stdlogw != nil {
			stdlogw.WriteString(logline)
			stdlogw.Flush()

		} else {
			slog.Infof("%s STDE %s >:%s", fun, m, logline)
		}
	}

    */
	slog.Infof("%s wait job:%s name:%s args:%s", fun, m, cmdname, cmdargs)
	// 只要返回status不是0就会有err
	if err := cmd.Wait(); err != nil {
		slog.Warnf("%s exit job:%s err:%s", fun, m, err)
		return err
	}

	slog.Infof("%s exit ok job:%s", fun, m)
	return nil
}

