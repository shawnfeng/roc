package rocserv

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"time"
)

func TestGetFuncTimeout(t *testing.T) {
	ass := assert.New(t)
	go Test([]string{"http://infra0.etcd.ibanyu.com:20002"}, "/roc", "base/servmonitor", func(xx ServBase) error {
		return nil
	})

	time.Sleep(2 * time.Second)

	timeout := GetFuncTimeout("base/servmonitor", "ReportRun", 6000*time.Second)
	ass.Equal(timeout, 6000*time.Second)
}
