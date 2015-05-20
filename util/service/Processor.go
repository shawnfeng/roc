// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv


type Processor interface {
	// init
	Init() error
	// interace driver
	Driver() (string, interface{})
}

