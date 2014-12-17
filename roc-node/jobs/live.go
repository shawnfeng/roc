package jobs

import (
	"github.com/shawnfeng/sutil/slog"
)

fun (m *Job) loadConf() ManulConf {

	return ManulConf{}
}

func (m *Job) live(ucmd chan *userCommand) {
	fun := "Job.live"

	runnf := make(chan *runNotify)
	exitst := make(chan *exitState)


	// 只能在func backoffRun 中使用
	backOff := stime.NewBackOffCtrl(time.Millisecond * 100, conf.BackOffCeil),
	backoffRun := func(flag int) {
		if flag == 0 {
			backOff.Reset()	
			m.run(runnf, exitst)
		} else if flag == 1 {
			backOff.BackOff()
			m.run(runnf, exitst)
		} else {
			m.run(runnf, exitst)
		}
	}



	// 是否已经启动了job run 的goroutine
	// 同一时间只能启动一个
	beenGorun := false
	setStop := false
	jobPid := 0
	for {
		select {
		case ucmd := <-ucmd
			slog.Infof("%s job:%s recv cmd:%s pid:%d gorun:%t stop:%t", fun, m, ucmd, jobPid, beenGorun, setStop)
			if ucmd.cmd == JOBCMD_START {
				setStop = false // 清除stop标记
				if beenGorun {
					slog.Warnf("%s job:%s have goroutine", fun, m)
				} else {
					go backoffRun(0)
					beenGorun = true
				}

			} else if (ucmd.cmd == JOBCMD_STOP) {
				setStop = true // 设置stop标记，运行的job停止后，将不能再被启动
				if jobPid == 0 {
					slog.Warnf("%s job:%s not run", fun, m)
				} else {
					// 将job杀掉，注意这里要启动一个go，否则你懂得。。。
					go func() {ucmd <- userCommand{cmd:JOBCMD_KILL}}()
				}

			} else if (ucmd.cmd == JOBCMD_KILL) {
				if jobPid == 0 {
					slog.Warnf("%s %s state not run", fun, m)
				} else {
					p, err := os.FindProcess(int(jobPid))
					if err != nil {
						slog.Warnf("%s %s find process err:%s", fun, m, err)
					} else {
						err = p.Kill()
						if err != nil {
							slog.Warnf("%s %s kill err:%s", fun, m, err)
							return err
						}
					}
				}

			} else {
				slog.Errorf("%s job:%s recv unknown cmd:%s", fun, m, ucmd)
			}
		// -----------------------
		case est := <-exitst
			// run job 退出了 gorun over
			conf := m.loadConf()
			slog.Warnf("%s job:%s exit:%s conf:%s pid:%d gorun:%t stop:%t", fun, m, est, &conf, jobPid, beenGorun, setStop)

			jobPid = 0
			beenGorun = false

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
				go backoffRun(0)
			} else {
				est.runDuration < 60*1000*1000*1000 {
					go backoffRun(2)
				} else {
					go backoffRun(0)
				}
			}
			beenGorun = true

		// -----------------------
		case rnf := <-runnf
			slog.Infof("%s job:%s runnotify:%s pid:%d gorun:%t stop:%t", fun, m, est, rnf, jobPid, beenGorun, setStop)
			jobPid = rnf.jobPid
			if m.cbProcessRun != nil {
				m.cbProcessRun(m)
			}
		}

	}
}




