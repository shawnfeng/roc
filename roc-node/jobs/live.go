package jobs

import (
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"bufio"
	"time"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
)

// test case
// start stop kill
// start start
// stop stop
// kill 后是否会重启
// remove 
// 没有start时候remove
// 测试remove后在start等操作
// 测试获取到pid时候，进程ps状态

// backoff配置测试
// backoff时候，start是否立即
// backoff时候kill
// backoff时候stop
// pid 获取
// 更新配置测试


//todo
   // OK job pid atomic
   // OK backoff sync
   // OK config load
   // OK jobs 打印增加pid输出
   // OK up config命令
   // OK backoff 重设置ceil
   // OK 重写 start stop kill upconfig命令
   // 重新过滤下代码逻辑

// 当前job状态加个记录
// pid 要不要直接在run里面设置？

// backoff期间收到kill指令？
// 没启动完收到kill
// backoff期间收到stop？
// start 时候应该停止backoff
// 实际是只要收到用户的指令就要停止backoff过程
func (m *Job) live(ucmds chan *userCommand) {
	fun := "Job.live"

	// 保护的是从启动job的goroutine到job成功获取pid区间
	// 不接收用户的命令
	var beenGetPid sync.WaitGroup

	exitst := make(chan *exitState)
	// 是否已经启动了job run 的goroutine
	// 同一时间只能启动一个
	beenGorun := false
	setStop := false
	removeFlag := false  //job 移除标记

	conf := m.getConf()
	backOff := stime.NewBackOffCtrl(time.Millisecond * 100, conf.BackOffCeil)

	// 启动失败定义：本次启动距离上次启动<60s，且不是因为接口控制的重启。成功一次计数清0,正常退出不算失败
	// 程序返回不是0
	// 内部错误也计入失败
	backoffRun := func(flag int) {
		lfun := "backoffRun"
		slog.Infof("%s %s begin job:%s", fun, lfun, m)
		if beenGorun {
			slog.Warnf("%s %s have been start goroutine job:%s", fun, lfun, m)
			return
		}

		beenGorun = true // 保证只有一个go run job启动

		beenGetPid.Add(1)
		slog.Infof("%s %s start goroutine add wait job:%s", fun, lfun, m)
		pidChan := make(chan int)
		go func() (ex *exitState) {
			defer func() {
				slog.Infof("%s %s over run job:%s", fun, lfun, m)
				exitst <- ex
			}()
			if flag == 0 {
				backOff.Reset()	
			} else if flag == 1 {
				backOff.BackOff()
			}
			stat := stime.NewTimeStat()

			slog.Infof("%s %s start run job:%s", fun, lfun, m)
			extp, err := m.run(&beenGetPid, pidChan, conf.Name, conf.Args...)

			return &exitState{runDuration:stat.Duration(), exitType: extp, runErr: err}
		}()

		go func() {
			pid := <-pidChan
			slog.Infof("%s pidChan wait job:%s pid:%d gorun:%t stop:%t", fun, m, pid, beenGorun, setStop)
			atomic.StoreInt32(&m.pid, int32(pid))
			if m.cbProcessRun != nil {
				m.cbProcessRun(m)
			}
		}()

	}


	killProc := func() {
		slog.Infof("%s killproc job:%s", fun, m)
		if atomic.LoadInt32(&m.pid) == 0 {
			slog.Warnf("%s killproc %s pid 0", fun, m)
		} else {
			p, err := os.FindProcess(int(atomic.LoadInt32(&m.pid)))
			if err != nil {
				slog.Warnf("%s killproc jobs:%s find process err:%s", fun, m, err)
			} else {
				err = p.Kill()
				if err != nil {
					slog.Warnf("%s killproc job:%s kill err:%s", fun, m, err)
				}
			}
		}

	}


	for {
		slog.Infof("%s select begin job:%s gorun:%t stop:%t remove:%t", fun, m, beenGorun, setStop, removeFlag)
		select {
		case ucmd := <-ucmds:
			slog.Infof("%s recv job:%s cmd:%s gorun:%t stop:%t", fun, m, ucmd.cmd, beenGorun, setStop)
			// 收到用户的命令，先终止backoff过程，然后等待
			backOff.Reset()
			beenGetPid.Wait()
			slog.Infof("%s waitover job:%s cmd:%s gorun:%t stop:%t", fun, m, ucmd.cmd, beenGorun, setStop)

			if ucmd.cmd == JOBCMD_START {
				setStop = false // 清除stop标记
				slog.Infof("%s cmd call backoffRun job:%s cmd:%s gorun:%t stop:%t", fun, m, ucmd.cmd, beenGorun, setStop)
				backoffRun(0)

			} else if (ucmd.cmd == JOBCMD_STOP) {
				setStop = true // 设置stop标记，运行的job停止后，将不能再被启动
				killProc()

			} else if (ucmd.cmd == JOBCMD_REMOVE) {
				// 退出循环
				removeFlag = true
				if !beenGorun {
					return
				} else {
					killProc()
				}

			} else if (ucmd.cmd == JOBCMD_KILL) {
				killProc()
			} else if (ucmd.cmd == JOBCMD_UPCONF) {
				conf = m.getConf()
				backOff.SetCtrl(time.Millisecond * 100, conf.BackOffCeil)
			} else {
				slog.Errorf("%s job:%s recv unknown cmd:%s", fun, m, ucmd)
			}
		// -----------------------
		case est := <-exitst:
			// run job 退出了 gorun over
			slog.Warnf("%s exit job:%s exit:%v conf:%s pid:%d gorun:%t stop:%t", fun, m, est, &conf, beenGorun, setStop)

			atomic.StoreInt32(&m.pid, 0)
			beenGorun = false

			if removeFlag {
				slog.Warnf("%s remove job:%s exit:%s pid:%d gorun:%t stop:%t", fun, m, est, beenGorun, setStop)
				return
			}

			if m.cbProcessStop != nil {
				m.cbProcessStop(m)
			}

			if setStop {
				// 已经设置了stop，不在启动
				break
			}

			if !conf.JobAuto {
				// 用户设置了非自动控制，不再启动
				break
			}

			// 此时需要重新启动了
			if est.exitType == Exit_OK {
				slog.Infof("%s exitok call backoffRun job:%s ex:%s gorun:%t stop:%t", fun, m, est.exitType, beenGorun, setStop)
				backoffRun(0)
			} else {
				if est.runDuration < 60*1000*1000*1000 {
					slog.Infof("%s exit short call backoffRun job:%s ex:%s gorun:%t stop:%t", fun, m, est.exitType, beenGorun, setStop)
					backoffRun(2)
				} else {
					slog.Infof("%s exit long call backoffRun job:%s ex:%s gorun:%t stop:%t", fun, m, est.exitType, beenGorun, setStop)
					backoffRun(0)
				}
			}

		}


		slog.Infof("%s select over job:%s gorun:%t stop:%t remove:%t", fun, m, beenGorun, setStop, removeFlag)
	}
}




func (m *Job) run(beenrun *sync.WaitGroup, pidChan chan int, cmdname string, cmdargs ...string) (jobExitType, error) {
	fun := "Job.run"

	cmd := exec.Command(cmdname, cmdargs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		beenrun.Done()
		slog.Errorf("%s over done stdio job:%s err:%s", fun, m, err)
		return Exit_Err_StdoutPipe, err
	}


	if err := cmd.Start(); err != nil {
		beenrun.Done()
		slog.Errorf("%s over done cmdstart job:%s err:%s", fun, m, err)
		return Exit_Err_CmdStart, err
	}



	slog.Infof("%s notify PID:%d %s", fun, cmd.Process.Pid, m)
	pidChan <- cmd.Process.Pid
	slog.Infof("%s done wait PID:%d %s", fun, cmd.Process.Pid, m)
	beenrun.Done()
	slog.Infof("%s over done wait PID:%d %s", fun, cmd.Process.Pid, m)

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

