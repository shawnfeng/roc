package rocserv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig/apollo"
	xmgo "gitlab.pri.ibanyu.com/middleware/seaweed/xmgo/manager"
	xsql "gitlab.pri.ibanyu.com/middleware/seaweed/xsql/manager"
)

func TestGetFuncTimeout(t *testing.T) {
	servLoc := "base/servmonitor"
	configCenter, err := xconfig.NewConfigCenter(context.Background(), apollo.ConfigTypeApollo, servLoc, []string{ApplicationNamespace, RPCConfNamespace, xsql.MysqlConfNamespace, xmgo.MongoConfNamespace})
	assert.Nil(t, err)

	go Test([]string{"http://infra0.etcd.ibanyu.com:20002"}, "/roc", servLoc, configCenter)

	time.Sleep(2 * time.Second)

	timeout := GetFuncTimeout("base/servmonitor", "ReportRun", 6000*time.Second)
	assert.Equal(t, timeout, 6000*time.Second)
}

func TestGetFuncRetry(t *testing.T) {
	servLoc := "base/servmonitor"
	configCenter, err := xconfig.NewConfigCenter(context.Background(), apollo.ConfigTypeApollo, servLoc, []string{ApplicationNamespace, RPCConfNamespace, xsql.MysqlConfNamespace, xmgo.MongoConfNamespace})
	assert.Nil(t, err)

	go Test([]string{"http://infra0.etcd.ibanyu.com:20002"}, "/roc", "base/servmonitor", configCenter)

	time.Sleep(2 * time.Second)

	retry := GetFuncRetry("base/test", "ReportLog")
	assert.Equal(t, 0, retry)
}
