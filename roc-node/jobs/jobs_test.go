package jobs

import (
	"testing"
	"fmt"
	"time"

	"github.com/shawnfeng/sutil/slog"
)



func TestContrl(t *testing.T) {
	slog.Init("", "", "TRACE")
	JobStart(t)
	//JobKill2(t)
	//JobLoop0(t)
	JobLoop1(t)
	//JobLoop2(t)
	//JobKill(t)
}

func JobStart(t *testing.T) {
	fun := "JobStart"

	mc := &ManulConf {
		"job0",
		"sh",
		[]string{"c.sh"},
		true,
		time.Second*60,
	}


	j := Newjob(mc)

	slog.Infoln(j)
	if j.String() != "job0@60000@sh[c.sh]true@STOP" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)
	}

	err := j.kill()
	if err != nil {
		slog.Infof("job kill error:%s", err)
	} else {
		slog.Errorln("job kill error")
		t.Errorf("%s", "job kill error")
	}

	j.start()
	time.Sleep(time.Millisecond * 100)
	slog.Infoln(j)
	if j.String() != "job0@60000@sh[c.sh]true@RUN" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}

	err = j.kill()
	if err != nil {
		slog.Errorln(fmt.Sprintf("job kill error %s", err))
		t.Errorf("%s", fmt.Sprintf("job kill error %s", err))

	}
	slog.Infoln(j)
	if j.String() != "job0@60000@sh[c.sh]true@RUN" {
		slog.Errorln(fun)
		t.Errorf("%s", fun)

	}

	// 停止自动重启
	mc.jobAuto = false
	j.updateConf(mc)
	err = j.kill()
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
		&ManulConf {
			"0",
			"sh",
			[]string{"c.sh"},
			false,
			time.Second*60,
		},
	)

	//go j.loop()

	time.Sleep(time.Second * 1)

	err := j.kill()
	if err != nil {
		t.Errorf("job kill error:%s", err)
	}

	err = j.kill()
	if err != nil {
		// os: process already finished
		slog.Infof("job kill2 error:%s", err)
	}


	time.Sleep(time.Second * 5)

}



func JobLoop0(t *testing.T) {

	j := Newjob(
		&ManulConf {
			"0",
			"sh",
			[]string{"c_notexist.sh"},
			true,
			time.Second*20,
		},
	)

	j.loop()
	//time.Sleep(time.Second * 5)

}

func JobLoop1(t *testing.T) {

	j := Newjob(
		&ManulConf {
			"0",
			"sh",
			[]string{"c_loop.sh"},
			true,
			time.Second*20,
		},
	)

	go j.loop()

	time.Sleep(time.Second * 2)
}

// 测试运行一段时间出错的情况，是否能够避免退避算法
func JobLoop2(t *testing.T) {

	j := Newjob(
		&ManulConf {
			"0",
			"sh",
			[]string{"c_exit1.sh"},
			true,
			time.Second*3,
		},
	)

	j.loop()
	
}


func JobKill(t *testing.T) {

	j := Newjob(
		&ManulConf {
			"0",
			"sh",
			[]string{"c.sh"},
			true,
			time.Second*20,
		},
	)

	err := j.kill()
	if err == nil {
		t.Errorf("job init kill error")
	}

	slog.Infof("%s", err)

	go j.loop()

	time.Sleep(time.Second * 5)

	err = j.kill()
	if err != nil {
		t.Errorf("job kill error:%s", err)
	}

	time.Sleep(time.Second * 5)

}

