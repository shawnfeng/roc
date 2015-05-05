package dbrouter

import (
	//"sync"
	"encoding/json"
)

const (
	DB_TYPE_MONGO = "mongo"
	DB_TYPE_MYSQL = "mysql"
)


type Config struct {
	Cluster map[string][]struct {
		Instance string  `json:"instance"`
		Express string   `json:"express"`
	} `json:"cluster"`

	Instances map[string]struct {
		Dbtype string             `json:"dbtype"`
		Dbname string             `json:"dbname"`
		//Dbcfg map[string]interface{}   `json:"dbcfg"`
		Dbcfg json.RawMessage      `json:"dbcfg"`
	} `json:"instances"`
}

type Router struct {
	dbCls *dbCluster
	dbIns *dbInstanceManager
}


func NewRouter(cfg *Config) (*Router, error) {
	r := &Router {
		dbCls: &dbCluster {
			clusters: make(map[string][]*dbExpress),
		},

		dbIns: &dbInstanceManager {
			instances: make(map[string]dbInstance),

		},
	}

	cls := cfg.Cluster
	for c, ins := range cls {
		for _, v := range ins {
			r.dbCls.addInstance(c, v.Instance, v.Express)
		}
	}

	inss := cfg.Instances
	for ins, db := range inss {
		tp := db.Dbtype
		dbname := db.Dbname
		cfg := db.Dbcfg
		// 工厂化构造，db类型领出来
		if tp == DB_TYPE_MONGO {
			dbi, err := NewdbMongo(tp, dbname, cfg)
			if err != nil {
				return nil, err
			}

			r.dbIns.add(ins, dbi)
		}

	}

	return r, nil
}



