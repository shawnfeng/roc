// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package dbrouter


import (
	"log"
	"fmt"
	"time"
	"encoding/json"
	"io/ioutil"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"testing"
)





func TestRouter(t *testing.T) {

	log.Println("test router")

	newrouter(t)
}

func newrouter(t *testing.T) {
	data, err := ioutil.ReadFile("./router.json")

	if err != nil {
		t.Errorf("config error")
		return
	}


	var cfg Config

    err = json.Unmarshal(data, &cfg)
	if err != nil {
		t.Errorf("config decode error:%s", err)
		return
	}

	log.Println(cfg)

	r, err := NewRouter(&cfg)
	if err != nil {
		t.Errorf("config router error:%s", err)
		return
	}
	log.Println(r, r.dbCls, r.dbIns)


	qf := func(c *mgo.Collection) {
		log.Printf("do c:%s", c)

	}

	err = r.MongoExecEventual("ACCOUNT", "fuck0", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecEventual("ACCOUNT", "fuck1", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecMonotonic("ACCOUNT", "fuck2", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecMonotonic("ACCOUNT", "fuck3", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecStrong("ACCOUNT", "fuck4", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecStrong("ACCOUNT", "fuck5", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}



	// ===============================
	qf1 := func(c *mgo.Collection) {
		log.Printf("do c:%s", c)

		_, err := c.Upsert(bson.M{"_id": 3},
			bson.M{"$set": bson.M{"a": time.Now().Unix()}},
		)

		if err != nil {
			t.Errorf("update error:%s", err)
		}
	}


	for i := 0; i < 11; i++ {

		err = r.MongoExecEventual("ACCOUNT", fmt.Sprintf("fuck%d", i), qf1)
		if err != nil {
			t.Errorf("do error:%s", err)
		}

	}


}

