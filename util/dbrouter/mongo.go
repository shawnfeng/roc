package dbrouter


import (
	"fmt"
	"time"
	"sync"
	//"reflect"
	"gopkg.in/mgo.v2"
	//"gopkg.in/mgo.v2/bson"

	"github.com/bitly/go-simplejson"
)


type dbMongo struct {
	dbType string
	dbName string
	dialInfo *mgo.DialInfo
	

	sessMu sync.RWMutex
	session [3]*mgo.Session

}

func (m *dbMongo) getType() string {
	return m.dbType
}

/*
func NewdbMongo(dbtype, dbname string, cfg map[string]interface{}) (*dbMongo, error) {

	iaddrs, ok := cfg["addrs"]
	if !ok {
		return nil, fmt.Errorf("mongo addrs config not find")
	}

	iaddrs2, ok := iaddrs.([]interface{})
	if !ok {
		return nil, fmt.Errorf("mongo addrs config not array is:%s", reflect.TypeOf(iaddrs))
	}

	addrs := make([]string, 0)
	for _, a := range iaddrs2 {
		addr, ok := a.(string)
		if !ok {
			return nil, fmt.Errorf("mongo addr config not string is:%s", reflect.TypeOf(a))
		} else {
			addrs = append(addrs, addr)
		}
	}


	timeout := 60 * time.Second

	if itimeout, ok := cfg["timeout"]; ok {
		t := reflect.ValueOf(itimeout).Int()
		//if !ok {
		//	return nil, fmt.Errorf("mongo timeout config not long is:%s", reflect.TypeOf(itimeout))
		//}
		timeout = time.Duration(t) * time.Millisecond
	}



	info := &mgo.DialInfo{
		Addrs: addrs,
		Timeout: timeout,
		Database: dbname,
		//Username: AuthUserName,
		//Password: AuthPassword,
	}
 


	return &dbMongo{
		dbType: dbtype,
		dbName: dbname,
		dialInfo: info,
	}, nil

}
*/

func NewdbMongo(dbtype, dbname string, cfg []byte) (*dbMongo, error) {

	cfg_json, err := simplejson.NewJson(cfg)
	if err != nil {
		return nil, err
	}

	addrs, err := cfg_json.Get("addrs").StringArray()
	if err != nil {
		return nil, err
	}

	timeout := 60 * time.Second
	if t, err := cfg_json.Get("timeout").Int64(); err == nil {
		timeout = time.Duration(t) * time.Millisecond
	}


	user, _ := cfg_json.Get("user").String()
	passwd, _ := cfg_json.Get("passwd").String()

	info := &mgo.DialInfo{
		Addrs: addrs,
		Timeout: timeout,
		Database: dbname,
		Username: user,
		Password: passwd,
	}


	return &dbMongo{
		dbType: dbtype,
		dbName: dbname,
		dialInfo: info,

	}, nil

}


type mode int

const (
	eventual  mode = 0
	monotonic mode = 1
	strong    mode = 2
)

func dialConsistency(info *mgo.DialInfo, consistency mode) (session *mgo.Session, err error) {

	session, err = mgo.DialWithInfo(info)
	if err != nil {
		return
	}
	// 看Dial内部的实现
	session.SetSyncTimeout(1 * time.Minute)
	// 不设置这个在执行写入，表不存在时候会报 read tcp 127.0.0.1:27017: i/o timeout
	session.SetSocketTimeout(1 * time.Minute)


	switch consistency {
	case eventual:
		session.SetMode(mgo.Eventual, true)
	case monotonic:
		session.SetMode(mgo.Monotonic, true)
	case strong:
		session.SetMode(mgo.Strong, true)
	}

	return
}


func dialConsistencyWithUrl(url string, timeout time.Duration, consistency mode) (session *mgo.Session, err error) {

	session, err = mgo.DialWithTimeout(url, timeout)
	if err != nil {
		return
	}
	// 看Dial内部的实现
	session.SetSyncTimeout(1 * time.Minute)
	// 不设置这个在执行写入，表不存在时候会报 read tcp 127.0.0.1:27017: i/o timeout
	session.SetSocketTimeout(1 * time.Minute)


	switch consistency {
	case eventual:
		session.SetMode(mgo.Eventual, true)
	case monotonic:
		session.SetMode(mgo.Monotonic, true)
	case strong:
		session.SetMode(mgo.Strong, true)
	}

	return
}



func (m *dbMongo) checkGetSession(consistency mode) *mgo.Session {
	m.sessMu.RLock()
	defer m.sessMu.RUnlock()

	return m.session[consistency]

}


func (m *dbMongo) initSession(consistency mode) (*mgo.Session, error) {
	m.sessMu.Lock()
	defer m.sessMu.Unlock()
	//fmt.Println("CCCCCC", m.session)

	if m.session[consistency] != nil {
		return m.session[consistency], nil
	} else {
		s, err := dialConsistency(m.dialInfo, consistency)
		if err != nil {
			return nil, err
		} else {
			m.session[consistency] = s
			return m.session[consistency], nil
		}
 	}
}

func (m *dbMongo) getSession(consistency mode) (*mgo.Session, error) {
	if s := m.checkGetSession(consistency); s != nil {
		return s, nil
	} else {
		return m.initSession(consistency)
	}
}



func (m *Router) mongoExec(consistency mode, cluster, table string, query func (*mgo.Collection)) error {

	ins_name := m.dbCls.getInstance(cluster, table)
	if ins_name == "" {
		return fmt.Errorf("cluster instance not find: cluster%s table:%s", cluster, table)
	}

	ins := m.dbIns.get(ins_name)
	if ins == nil {
		return fmt.Errorf("db instance not find: cluster%s table:%s", cluster, table)
	}

	db, ok := ins.(*dbMongo)
	if !ok {
		return fmt.Errorf("db instance type error: cluster%s table:%s type:%s", cluster, table, ins.getType())
	}
	
	sess, err := db.getSession(consistency)
	if err != nil {
		return err
	}

	if sess == nil {
		return fmt.Errorf("db instance session empty: cluster%s table:%s type:%s", cluster, table, ins.getType())
	}



	sessionCopy := sess.Copy()
	defer sessionCopy.Close()
	c := sessionCopy.DB("").C(table)

	query(c)

	return nil
}

func (m *Router) MongoExecEventual(cluster, table string, query func (*mgo.Collection)) error {
	return m.mongoExec(eventual, cluster, table, query)
}

func (m *Router) MongoExecMonotonic(cluster, table string, query func (*mgo.Collection)) error {
	return m.mongoExec(monotonic, cluster, table, query)
}

func (m *Router) MongoExecStrong(cluster, table string, query func (*mgo.Collection)) error {
	return m.mongoExec(strong, cluster, table, query)
}







