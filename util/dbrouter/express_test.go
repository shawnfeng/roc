// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package dbrouter


import (
	"log"
	"testing"
)





func TestExpress(t *testing.T) {

	clustertest(t)
}


func check_ins(t *testing.T, dbs *dbCluster, cluster, table, ins_res string) {


	ins := dbs.getInstance(cluster, table)
	log.Println("ins", cluster, table, ins, ins_res)
	if ins != ins_res {
		t.Errorf("err c:%s t:%s ins:%s res:%s", cluster, table, ins, ins_res)
	}

}

func clustertest(t *testing.T) {

	dbs := &dbCluster {
		clusters: make(map[string][]*dbExpress),
	}

	cluster := "account"

	err := dbs.addInstance(cluster, "user", "user[0-5]")
	if err != nil {
		t.Errorf("err add:%s", err)
	}

	err = dbs.addInstance(cluster, "auth", "auth[0-9]+")
	if err != nil {
		t.Errorf("err add:%s", err)
	}


	check_ins(t, dbs, cluster, "user0", "user")
	check_ins(t, dbs, cluster, "user1", "user")
	check_ins(t, dbs, cluster, "user2", "user")

	check_ins(t, dbs, cluster, "user_not", "")


	check_ins(t, dbs, cluster, "auth", "")
	check_ins(t, dbs, cluster, "auth0", "auth")
	check_ins(t, dbs, cluster, "auth1", "auth")
	check_ins(t, dbs, cluster, "auth99", "auth")
	check_ins(t, dbs, cluster, "auth01", "auth")

}
