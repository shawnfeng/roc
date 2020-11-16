// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package headope

import (
	"fmt"
	"testing"
	"time"

	"github.com/shawnfeng/sutil/slog"
	"net/http"
)

func master(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s", "127.0.0.1:9000")
}

func servHttp(t *testing.T) {
	http.HandleFunc("/get/master", master)
	err := http.ListenAndServe(":9000", nil)

	if err != nil {
		t.Errorf("httpserv %s", err)
	}
}

func TestIt(t *testing.T) {
	go servHttp(t)
	TTimeout(t)
	TEmpty(t)
	TTOk(t)
}

func TTOk(t *testing.T) {

	// allow err
	headAddrs0 := []string{"127.0.0.1:9100", "127.0.0.1:9000", "127.0.0.1:9102"}

	h, err := findMaster(headAddrs0, time.Second*5)

	if err != nil {
		t.Errorf("%s", err)
	}

	slog.Infof("head ok h:%s", h)
}

func TTimeout(t *testing.T) {

	// allow err
	headAddrs0 := []string{"127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102"}

	h, err := findMaster(headAddrs0, time.Second*5)

	if err == nil || err != TimeoutErr {
		t.Errorf("%s", err)
	}

	slog.Infof("head timeout h:%s err:%s", h, err)
}

func TEmpty(t *testing.T) {
	// allow err
	headAddrs0 := []string{}

	h, err := findMaster(headAddrs0, time.Second*5)

	if err == nil || err != EmptyAddrErr {
		t.Errorf("%s", err)
	}

	slog.Infof("head empty h:%s err:%s", h, err)
}
