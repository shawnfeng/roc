// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig/apollo"
	xmgo "gitlab.pri.ibanyu.com/middleware/seaweed/xmgo/manager"
	xsql "gitlab.pri.ibanyu.com/middleware/seaweed/xsql/manager"
	"gitlab.pri.ibanyu.com/middleware/util/servbase"
)

func TestIt(t *testing.T) {
	args := &cmdArgs{
		servLoc:           "niubi/fuck",
		sessKey:           "7e07d3e6-2737-43ac-86fa-157bc1bb8943a",
		group:             "",
		crossRegionIdList: "",
	}

	configCenter, err := xconfig.NewConfigCenter(context.TODO(), apollo.ConfigTypeApollo, args.servLoc, []string{ApplicationNamespace, RPCConfNamespace, xsql.MysqlConfNamespace, xmgo.MongoConfNamespace})
	assert.Nil(t, err)

	sb, err := newServBaseV2WithCmdArgs(configEtcd{servbase.ETCDS_CLUSTER_0, "/roc"}, args, configCenter)
	if err != nil {
		t.Errorf("create err:%s", err)
		return
	}

	log.Println(sb)

	type TConf2 struct {
		Uname  string
		Passwd string
		Fuck   int
		Girl   int64

		// 不是指针的、是指针的，指针为空的或者不为空的
		Ts *struct {
			AAA string
			BBB uint8

			CCC bool

			LLL []int `sep:"," sconf:"lll"`
			M   map[string][]string
		}

		Ts1 *string

		Sm map[string]struct {
			Ee string
			Ff string
		}
	}

	var svconf TConf2

	err = sb.ServConfig(&svconf)
	if err != nil {
		t.Errorf("serv config err:%s", err)
		return
	}

	log.Println(svconf)

	sb.Lock("testlock")
	isl, err := sb.Trylock("testlock")
	log.Println("trylock", isl, err)
	time.Sleep(time.Second * 50)
	sb.Unlock("testlock")
	//time.Sleep(time.Second*2)
	//sb.Unlock("testlock")

	// =================

	sb.LockGlobal("testlock")
	isl, err = sb.TrylockGlobal("testlock")
	log.Println("trylock", isl, err)
	time.Sleep(time.Second * 2)
	sb.UnlockGlobal("testlock")

	time.Sleep(time.Second * 50)
	sb.Lock("testlock")
	time.Sleep(time.Second * 50)
}
