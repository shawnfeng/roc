package jobs

import (
	"testing"
	"strings"
	"fmt"
	"time"

	"github.com/shawnfeng/sutil/slog"
)


// test case
// start stop kill
// stop stop
// kill 后是否会重启
// start 马上stop能起到作用不
// stop 后start

// start 马上remove能起到作用不
// 没有start时候remove
// 测试remove后在start等操作


// start start

// 测试获取到pid时候，进程ps状态

// backoff配置测试
// backoff时候，start是否立即
// backoff时候kill
// backoff时候stop
// pid 获取
// 更新配置测试
// callback测试


func TestContrl(t *testing.T) {
	//JobStart(t)
	//JobStopKill(t)
	//JobStart2(t)
	JobStart3(t)

}

func JobStart3(t *testing.T) {
	fun := "JobStart3"

	mc := &ManulConf {
		"sh",
		[]string{"c.sh"},
		true,
		time.Second*60,
	}

	cbstart := func(pid int32, j *Job) {
		slog.Infoln("@@callback job start:", pid, j)

	}

	cbstop := func(pid int32, j *Job) {
		slog.Infoln("@@callback job stop:", pid, j)

	}

	j := Newjob("job0", mc, cbstop, cbstart)

	err := j.Start()
	if err != nil {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	slog.Infoln(j)
	if j.String() != "job0|0" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	err = j.Stop()
	// 此时应该job还没有来得及start
	if err != nil {
		t.Errorf("here")
	}

	// 看看start 后马上stop，job应该不能启动
	time.Sleep(time.Second * 2)
	if j.String() != "job0|0" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	} else {
		slog.Infoln("ok not start")
	}

	err = j.Start()
	if err != nil {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	time.Sleep(time.Second * 1)
	// 应该启动
	if j.String() == "job0|0" {
		t.Errorf("%s", fun)
	} else {
		slog.Infoln("ok start", j.String())
	}



}

func JobStart2(t *testing.T) {
	fun := "JobStart2"

	mc := &ManulConf {
		"sh",
		[]string{"c.sh"},
		true,
		time.Second*60,
	}

	cbstart := func(pid int32, j *Job) {
		slog.Infoln("@@callback job start:", pid, j)

	}

	cbstop := func(pid int32, j *Job) {
		slog.Infoln("@@callback job stop:", pid, j)

	}

	j := Newjob("job0", mc, cbstop, cbstart)
	// 看看stop backoff的log
	time.Sleep(time.Second * 5)

	err := j.Start()
	if err != nil {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	slog.Infoln(j)
	if j.String() != "job0|0" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}


	err = j.Kill()
	// 此时应该job还没有来得及start
	if err == nil {
		t.Errorf("here")
	}
	time.Sleep(time.Millisecond * 100)
	// 停一会，让启动了再kill
	err = j.Kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))

	}
	// 给自动重启一个时间间隔
	time.Sleep(time.Millisecond * 1000)
	slog.Infoln(j)
	err = j.Kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))

	}
	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}
	time.Sleep(time.Millisecond * 1000)
	slog.Infoln("=======================")
	// 停止自动重启
	mc.JobAuto = false
	j.updateConf(mc)
	err = j.Kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))
	}
	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}
	time.Sleep(time.Second * 21)
	// 应该没有自动重启
	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun, j)
		t.Errorf("%s %s", fun, j)

	}

}


func JobStart(t *testing.T) {
	fun := "JobStart"

	mc := &ManulConf {
		"sh",
		[]string{"c.sh"},
		true,
		time.Second*60,
	}


	j := Newjob("job0", mc, nil, nil)


	slog.Infoln(j)
	if j.String() != "job0|0" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	err := j.Kill()
	if err != nil {
		slog.Errorln("job kill error")
		t.Errorf("%s", "job kill error")
	}

	err = j.Start()
	if err != nil {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}
	time.Sleep(time.Millisecond * 100)
	slog.Infoln(j)

	err = j.Kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))

	}
	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}
	slog.Infoln("=======================")
	// 停止自动重启
	mc.JobAuto = false
	j.updateConf(mc)
	err = j.Kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))
	}
	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}
	time.Sleep(time.Millisecond * 1000)

	slog.Infoln(j)
	if strings.Index(j.String(), "job0|") == -1 {
		slog.Errorln(fun, j)
		t.Errorf("%s %s", fun, j)

	}

}

// 测试process 被kill两次的行为
// 重复的kill会出现 os: process already finished
func JobKill2(t *testing.T) {

	j := Newjob(
		"0",
		&ManulConf {
			"sh",
			[]string{"c.sh"},
			false,
			time.Second*60,
		},
		nil, nil,
	)


	time.Sleep(time.Second * 1)

	err := j.Kill()
	if err != nil {
		t.Errorf("job kill error:%s", err)
	}

	err = j.Kill()
	if err != nil {
		// os: process already finished
		slog.Infof("job kill2 error:%s", err)
	}


	time.Sleep(time.Second * 5)

}



func JobLoop0(t *testing.T) {

	_ = Newjob(
			"0",
		&ManulConf {
			"sh",
			[]string{"c_notexist.sh"},
			true,
			time.Second*20,
		},
		nil, nil,
	)


}

func JobLoop1(t *testing.T) {

	_ = Newjob(
			"0",
		&ManulConf {
			"sh",
			[]string{"c_loop.sh"},
			true,
			time.Second*20,
		},
		nil, nil,
	)


	time.Sleep(time.Second * 2)
}

// 测试运行一段时间出错的情况，是否能够避免退避算法
func JobLoop2(t *testing.T) {

	_ = Newjob(
			"0",
		&ManulConf {
			"sh",
			[]string{"c_exit1.sh"},
			true,
			time.Second*3,
		},
		nil, nil,
	)

	
}


func JobStopKill(t *testing.T) {

	j := Newjob(
			"0",
		&ManulConf {
			"sh",
			[]string{"c.sh"},
			true,
			time.Second*20,
		},
		nil, nil,
	)

	err := j.Kill()
	if err == nil {
		t.Errorf("job init kill error")
	}

	slog.Infof("%s", err)

	for i := 0; i < 20; i++ {
		go func() {
			err := j.Kill()
			if err == nil {
				t.Errorf("job init kill error")
			}
			j.Stop()
		}()
	}

	time.Sleep(time.Second)

}

