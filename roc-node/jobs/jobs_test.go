package jobs

import (
	"testing"
	"fmt"
	"time"

	"github.com/shawnfeng/sutil/slog"
)



func TestContrl(t *testing.T) {
	JobStart(t)
	//JobKill2(t)
	//JobLoop0(t)
	//JobLoop1(t)
	//JobLoop2(t)
	//JobKill(t)
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
	if j.String() != "job0|60000|true|sh|[c.sh]|0" {
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
	if j.String() != "job0@60000@sh[c.sh]true@RUN" {
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
	if j.String() != "job0@60000@sh[c.sh]false@RUN" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}
	time.Sleep(time.Millisecond * 1000)

	slog.Infoln(j)
	if j.String() != "job0@60000@sh[c.sh]false@STOP" {
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


func JobKill(t *testing.T) {

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


	time.Sleep(time.Second * 5)

	err = j.Kill()
	if err != nil {
		t.Errorf("job kill error:%s", err)
	}

	time.Sleep(time.Second * 5)

}

