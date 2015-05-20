// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package dbrouter

import (
	//"sync"
)


type dbInstance interface {
	getType() string
}

type dbInstanceManager struct {
	instances map[string]dbInstance
}


func (m *dbInstanceManager) add(name string, ins dbInstance) {
	m.instances[name] = ins
}


func (m *dbInstanceManager) get(name string) dbInstance {
	ins, _ := m.instances[name]
	return ins
}




