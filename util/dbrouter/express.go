// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package dbrouter

import (
	//"sync"
	"regexp"
)


// 没有考虑同步问题
// 目前只支持初始化一次加载完成
// 构建完成后不能动态调整
type dbCluster struct {
	//clsMu sync.Mutex
	clusters map[string][]*dbExpress
}


type dbExpress struct {
	ins string
	reg *regexp.Regexp
}


func (m *dbCluster) addInstance(cluster, ins, expr string) error {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return err
	}

	//m.clsMu.Lock()
	//defer m.clsMu.Unlock()

	if _, ok := m.clusters[cluster]; !ok {
		m.clusters[cluster] = make([]*dbExpress, 0)
	}

	m.clusters[cluster] = append(m.clusters[cluster], &dbExpress{ins, reg})

	return nil
}

func (m *dbCluster) getInstance(cluster string, table string) string {
	//m.clsMu.Lock()
	//defer m.clsMu.Unlock()

	if v, ok := m.clusters[cluster]; ok {
		for _, e := range v {
			if e.reg.MatchString(table) {
				return e.ins
			}
		}
	}

	return ""
}
