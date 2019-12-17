// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"flag"
	"fmt"
	"reflect"
	"sync"

	stat "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/sys"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/gin-gonic/gin"
	"github.com/julienschmidt/httprouter"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/slog/statlog"
	"github.com/shawnfeng/sutil/trace"

	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
)

const (
	PROCESSOR_HTTP   = "http"
	PROCESSOR_THRIFT = "thrift"
	PROCESSOR_GRPC   = "gprc"
	PROCESSOR_GIN    = "gin"

	MODEL_SERVER      = 0
	MODEL_MASTERSLAVE = 1
)

var service = NewService()

type Service struct {
	sbase ServBase

	mutex   sync.Mutex
	servers map[string]interface{}
}

func NewService() *Service {
	return &Service{
		servers: make(map[string]interface{}),
	}
}

type cmdArgs struct {
	logMaxSize    int
	logMaxBackups int
	servLoc       string
	logDir        string
	sessKey       string
	group         string
	disable       bool
	model         int
}

func (m *Service) parseFlag() (*cmdArgs, error) {
	var serv, logDir, skey, group string
	var logMaxSize, logMaxBackups int
	flag.IntVar(&logMaxSize, "logmaxsize", 0, "logMaxSize is the maximum size in megabytes of the log file")
	flag.IntVar(&logMaxBackups, "logmaxbackups", 0, "logmaxbackups is the maximum number of old log files to retain")
	flag.StringVar(&serv, "serv", "", "servic name")
	flag.StringVar(&logDir, "logdir", "", "serice log dir")
	flag.StringVar(&skey, "skey", "", "service session key")
	flag.StringVar(&group, "group", "", "service group")

	flag.Parse()

	if len(serv) == 0 {
		return nil, fmt.Errorf("serv args need!")
	}

	if len(skey) == 0 {
		return nil, fmt.Errorf("skey args need!")
	}

	return &cmdArgs{
		logMaxSize:    logMaxSize,
		logMaxBackups: logMaxBackups,
		servLoc:       serv,
		logDir:        logDir,
		sessKey:       skey,
		group:         group,
	}, nil

}

func (m *Service) loadDriver(sb ServBase, procs map[string]Processor) (map[string]*ServInfo, error) {
	fun := "Service.loadDriver -->"

	infos := make(map[string]*ServInfo)

	for n, p := range procs {
		addr, driver := p.Driver()
		if driver == nil {
			slog.Infof("%s processor:%s no driver", fun, n)
			continue
		}

		slog.Infof("%s processor:%s type:%s addr:%s", fun, n, reflect.TypeOf(driver), addr)

		switch d := driver.(type) {
		case *httprouter.Router:
			sa, err := powerHttp(addr, d)
			if err != nil {
				return nil, err
			}

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo{
				Type: PROCESSOR_HTTP,
				Addr: sa,
			}

		case thrift.TProcessor:
			sa, err := powerThrift(addr, d)
			if err != nil {
				return nil, err
			}

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo{
				Type: PROCESSOR_THRIFT,
				Addr: sa,
			}
		case *GrpcServer:
			sa, err := powerGrpc(addr, d)
			if err != nil {
				return nil, err
			}

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo{
				Type: PROCESSOR_GRPC,
				Addr: sa,
			}
		case *gin.Engine:
			sa, serv, err := powerGin(addr, d)
			if err != nil {
				return nil, err
			}

			m.addServer(n, serv)

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo{
				Type: PROCESSOR_GIN,
				Addr: sa,
			}
		default:
			return nil, fmt.Errorf("processor:%s driver not recognition", n)

		}
	}

	return infos, nil
}

func (m *Service) addServer(processor string, server interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.servers[processor] = server
}

func (m *Service) reloadRouter(processor string, driver interface{}) error {
	//fun := "Service.reloadRouter -->"

	m.mutex.Lock()
	defer m.mutex.Unlock()
	server, ok := m.servers[processor]
	if !ok {
		return fmt.Errorf("processor:%s driver not recognition", processor)
	}

	return reloadRouter(processor, server, driver)
}

func (m *Service) Serve(confEtcd configEtcd, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Service.Serve -->"

	args, err := m.parseFlag()
	if err != nil {
		slog.Panicf("%s parse arg err:%s", fun, err)
		return err
	}

	return m.Init(confEtcd, args, initfn, procs)
}

func (m *Service) initLog(sb *ServBaseV2, args *cmdArgs) error {
	fun := "Service.initLog -->"

	logDir := args.logDir
	var logConfig struct {
		Log struct {
			Level string
			Dir   string
		}
	}
	logConfig.Log.Level = "INFO"

	err := sb.ServConfig(&logConfig)
	if err != nil {
		slog.Errorf("%s serv config err:%s", fun, err)
		return err
	}

	var logdir string
	if len(logConfig.Log.Dir) > 0 {
		logdir = fmt.Sprintf("%s/%s", logConfig.Log.Dir, sb.Copyname())
	}

	if len(logDir) > 0 {
		logdir = fmt.Sprintf("%s/%s", logDir, sb.Copyname())
	}

	if logDir == "console" {
		logdir = ""
	}

	slog.Infof("%s init log dir:%s name:%s level:%s", fun, logdir, args.servLoc, logConfig.Log.Level)

	slog.Init(logdir, "serv.log", logConfig.Log.Level)
	statlog.Init(logdir, "stat.log", args.servLoc)
	return nil
}

func (m *Service) Init(confEtcd configEtcd, args *cmdArgs, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Service.Init -->"

	servLoc := args.servLoc
	sessKey := args.sessKey

	sb, err := NewServBaseV2(confEtcd, servLoc, sessKey, args.group)
	if err != nil {
		slog.Panicf("%s init servbase loc:%s key:%s err:%s", fun, servLoc, sessKey, err)
		return err
	}
	m.sbase = sb

	m.initLog(sb, args)

	//服务进程打点
	stat.Init(sb.servGroup, sb.servName, "")

	defer slog.Sync()
	defer statlog.Sync()

	err = m.handleModel(sb, servLoc, args.model)
	if err != nil {
		slog.Panicf("%s handleModel err:%s", fun, err)
		return err
	}

	err = initfn(sb)
	if err != nil {
		slog.Panicf("%s callInitFunc err:%s", fun, err)
		return err
	}

	// NOTE: processor 在初始化 trace middleware 前需要保证 opentracing.GlobalTracer() 初始化完毕
	m.initTracer(servLoc)

	err = m.initProcessor(sb, procs)
	if err != nil {
		slog.Panicf("%s initProcessor err:%s", fun, err)
		return err
	}

	sb.SetGroupAndDisable(args.group, args.disable)

	m.initBackdoork(sb)
	m.initMetric(sb)

	var pause chan bool
	pause <- true

	return nil
}

func (m *Service) handleModel(sb *ServBaseV2, servLoc string, model int) error {
	fun := "Service.handleModel -->"

	if model == MODEL_MASTERSLAVE {
		lockKey := fmt.Sprintf("%s-master-slave", servLoc)
		if err := sb.LockGlobal(lockKey); err != nil {
			slog.Errorf("%s LockGlobal key: %s, err: %s", fun, lockKey, err)
			return err
		}

		slog.Infof("%s LockGlobal succ, key: %s", fun, lockKey)
	}

	return nil
}

func (m *Service) initProcessor(sb *ServBaseV2, procs map[string]Processor) error {
	fun := "Service.initProcessor -->"

	for n, p := range procs {
		if len(n) == 0 {
			slog.Errorf("%s processor name empty", fun)
			return fmt.Errorf("processor name empty")
		}

		if n[0] == '_' {
			slog.Errorf("%s processor name can not prefix '_'", fun)
			return fmt.Errorf("processor name can not prefix '_'")
		}

		if p == nil {
			slog.Errorf("%s processor:%s is nil", fun, n)
			return fmt.Errorf("processor:%s is nil", n)
		} else {
			err := p.Init()
			if err != nil {
				slog.Errorf("%s processor:%s init err:%s", fun, err)
				return fmt.Errorf("processor:%s init err:%s", n, err)
			}
		}
	}

	infos, err := m.loadDriver(sb, procs)
	if err != nil {
		slog.Errorf("%s load driver err:%s", fun, err)
		return err
	}

	err = sb.RegisterService(infos)
	if err != nil {
		slog.Errorf("%s regist service err:%s", fun, err)
		return err
	}

	return nil
}

func (m *Service) initTracer(servLoc string) error {
	fun := "Service.initTracer -->"

	err := trace.InitDefaultTracer(servLoc)
	if err != nil {
		slog.Errorf("%s init tracer fail:%v", fun, err)
	}

	return err
}

func (m *Service) initBackdoork(sb *ServBaseV2) error {
	fun := "Service.initBackdoork -->"

	backdoor := &backDoorHttp{}
	err := backdoor.Init()
	if err != nil {
		slog.Errorf("%s init backdoor err:%s", fun, err)
		return err
	}

	binfos, err := m.loadDriver(sb, map[string]Processor{"_PROC_BACKDOOR": backdoor})
	if err == nil {
		err = sb.RegisterBackDoor(binfos)
		if err != nil {
			slog.Errorf("%s regist backdoor err:%s", fun, err)
		}

	} else {
		slog.Warnf("%s load backdoor driver err:%s", fun, err)
	}

	return err
}

func (m *Service) initMetric(sb *ServBaseV2) error {
	fun := "Service.initMetric -->"

	metrics := xprom.NewMetricProcessor()
	err := metrics.Init()
	if err != nil {
		slog.Warnf("%s init metrics err:%s", fun, err)
	}

	minfos, err := m.loadDriver(sb, map[string]Processor{"_PROC_METRICS": metrics})
	if err == nil {
		err = sb.RegisterMetrics(minfos)
		if err != nil {
			slog.Warnf("%s regist backdoor err:%s", fun, err)
		}

	} else {
		slog.Warnf("%s load metrics driver err:%s", fun, err)
	}
	return err
}

func ReloadRouter(processor string, driver interface{}) error {
	return service.reloadRouter(processor, driver)
}

func Serve(etcds []string, baseLoc string, initfn func(ServBase) error, procs map[string]Processor) error {
	return service.Serve(configEtcd{etcds, baseLoc}, initfn, procs)
}

func MasterSlave(etcds []string, baseLoc string, initfn func(ServBase) error, procs map[string]Processor) error {
	return service.MasterSlave(configEtcd{etcds, baseLoc}, initfn, procs)
}

func (m *Service) MasterSlave(confEtcd configEtcd, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Service.MasterSlave -->"

	args, err := m.parseFlag()
	if err != nil {
		slog.Panicf("%s parse arg err:%s", fun, err)
		return err
	}
	args.model = MODEL_MASTERSLAVE

	return m.Init(confEtcd, args, initfn, procs)
}

func Init(etcds []string, baseLoc string, servLoc, servKey, logDir string, initfn func(ServBase) error, procs map[string]Processor) error {
	args := &cmdArgs{
		logMaxSize:    0,
		logMaxBackups: 0,
		servLoc:       servLoc,
		logDir:        logDir,
		sessKey:       servKey,
	}
	return service.Init(configEtcd{etcds, baseLoc}, args, initfn, procs)
}

func GetServBase() ServBase {
	return service.sbase
}

func GetServName() (servName string) {
	if service.sbase != nil {
		servName = service.sbase.Servname()
	}
	return
}
func GetServId() (servId int) {
	if service.sbase != nil {
		servId = service.sbase.Servid()
	}
	return
}

func Test(etcds []string, baseLoc, servLoc string, initfn func(ServBase) error) error {
	args := &cmdArgs{
		logMaxSize:    0,
		logMaxBackups: 0,
		servLoc:       servLoc,
		sessKey:       "test",
		logDir:        "console",
		disable:       true,
	}
	return service.Init(configEtcd{etcds, baseLoc}, args, initfn, nil)
}
