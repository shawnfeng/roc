// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
    "github.com/julienschmidt/httprouter"

	//"github.com/shawnfeng/sutil/slog"
	//"github.com/shawnfeng/sutil/snetutil"

)

type backDoorHttp struct {}

func (m *backDoorHttp) Init() error {
	return nil
}

func (m *backDoorHttp) Driver() (string, interface{}) {
	//fun := "backDoorHttp.Driver -->"

    router := httprouter.New()

	return "", router

}
