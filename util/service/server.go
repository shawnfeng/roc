// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"gitlab.pri.ibanyu.com/middleware/dolphin/circuit_breaker"
	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit/registry"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	stat "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/sys"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"
	"gitlab.pri.ibanyu.com/middleware/util/servbase"
)

// server model
const (
	MODEL_SERVER      = 0
	MODEL_MASTERSLAVE = 1
	START_TYPE_LOCAL  = "local"
)

var server = NewServer()

// Server ...
type Server struct {
	sbase ServBase
}

// NewServer create new server
func NewServer() *Server {
	return &Server{}
}

type cmdArgs struct {
	logMaxSize        int
	logMaxBackups     int
	servLoc           string
	logDir            string
	sessKey           string
	sidOffset         int
	group             string
	disable           bool
	model             int
	startType         string // 启动方式：local - 不注册至etcd
	crossRegionIdList string
}

func (m *Server) parseFlag() (*cmdArgs, error) {
	var serv, logDir, skey, group, startType string
	var logMaxSize, logMaxBackups, sidOffset int
	flag.IntVar(&logMaxSize, "logmaxsize", 0, "logMaxSize is the maximum size in megabytes of the log file")
	flag.IntVar(&logMaxBackups, "logmaxbackups", 0, "logmaxbackups is the maximum number of old log files to retain")
	flag.StringVar(&serv, "serv", "", "servic name")
	flag.StringVar(&logDir, "logdir", "", "serice log dir")
	flag.StringVar(&skey, "skey", "", "service session key")
	flag.IntVar(&sidOffset, "sidoffset", 0, "service id offset for different data center")
	flag.StringVar(&group, "group", "", "service group")
	// 启动方式：local - 不注册至etcd
	flag.StringVar(&startType, "stype", "", "start up type, local is not register to etcd")

	flag.Parse()

	if len(serv) == 0 {
		return nil, fmt.Errorf("serv args need!")
	}

	if len(skey) == 0 {
		return nil, fmt.Errorf("skey args need!")
	}

	crossRegionIdList := os.Getenv("CROSSREGIONIDLIST")

	return &cmdArgs{
		logMaxSize:        logMaxSize,
		logMaxBackups:     logMaxBackups,
		servLoc:           serv,
		logDir:            logDir,
		sessKey:           skey,
		sidOffset:         sidOffset,
		group:             group,
		startType:         startType,
		crossRegionIdList: crossRegionIdList,
	}, nil

}

func (m *Server) loadDriver(procs map[string]Processor) (map[string]*ServInfo, error) {
	fun := "Server.loadDriver -->"
	ctx := context.Background()

	infos := make(map[string]*ServInfo)

	for name, processor := range procs {
		driverBuilder := newDriverBuilder(m.sbase.ConfigCenter())
		servInfo, err := driverBuilder.powerProcessorDriver(ctx, name, processor)
		if err == errNilDriver {
			xlog.Infof(ctx, "%s processor: %s no driver, skip", fun, name)
			continue
		}
		if err != nil {
			xlog.Errorf(ctx, "%s load error, processor: %s, err: %v", fun, name, err)
			return nil, err
		}

		infos[name] = servInfo
		xlog.Infof(ctx, "%s load ok, processor: %s, serv addr: %s", fun, name, servInfo.Addr)
	}

	return infos, nil
}

// Serve handle request and return response
func (m *Server) Serve(confEtcd configEtcd, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Server.Serve -->"

	args, err := m.parseFlag()
	if err != nil {
		xlog.Panicf(context.Background(), "%s parse arg err: %v", fun, err)
		return err
	}

	return m.Init(confEtcd, args, initfn, procs)
}

func (m *Server) initLog(sb *ServBaseV2, args *cmdArgs) error {
	fun := "Server.initLog -->"

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
		xlog.Errorf(context.Background(), "%s serv config err: %v", fun, err)
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

	xlog.Infof(context.Background(), "%s init log dir:%s name:%s level:%s", fun, logdir, args.servLoc, logConfig.Log.Level)

	// 最终根据Apollo中配置的log level决定日志级别， TODO 后续将从etcd获取日志配置的逻辑去掉，统一在Apollo内配置
	logLevel, ok := m.sbase.ConfigCenter().GetString(context.TODO(), "log_level")
	if ok {
		logConfig.Log.Level = logLevel
	}
	xlog.InitAppLog(logdir, "serv.log", convertLevel(logConfig.Log.Level))
	xlog.InitStatLog(logdir, "stat.log")
	xlog.SetStatLogService(args.servLoc)
	return nil
}

func (m *Server) Init(confEtcd configEtcd, args *cmdArgs, initfn func(ServBase) error, procs map[string]Processor) error {
	ctx := context.Background()
	fun := "Server.Init -->"

	servLoc := args.servLoc
	sessKey := args.sessKey
	crossRegionIdList, err := parseCrossRegionIdList(args.crossRegionIdList)
	if err != nil {
		xlog.Panicf(ctx, "%s parse cross region id list error, arg: %v, err: %v", fun, args.crossRegionIdList, err)
		return err
	}
	xlog.Infof(ctx, "%s new ServBaseV2 start", fun)
	sb, err := NewServBaseV2(confEtcd, servLoc, sessKey, args.group, args.sidOffset, crossRegionIdList)
	if err != nil {
		xlog.Panicf(ctx, "%s init servbase loc: %s key: %s err: %v", fun, servLoc, sessKey, err)
		return err
	}
	m.sbase = sb
	xlog.Infof(ctx, "%s new ServBaseV2 end", fun)

	m.sbase.SetStartType(args.startType)

	//将ip存储
	if err := sb.setIp(); err != nil {
		xlog.Errorf(ctx, "%s set ip error: %v", fun, err)
	}

	// 初始化日志
	xlog.Infof(ctx, "%s initLog start", fun)
	m.initLog(sb, args)
	xlog.Infof(ctx, "%s initLog end", fun)

	// 初始化服务进程打点
	xlog.Infof(ctx, "%s init stat start", fun)
	stat.Init(sb.servGroup, sb.servName, "")
	xlog.Infof(ctx, "%s init stat end", fun)

	defer xlog.AppLogSync()
	defer xlog.StatLogSync()

	// NOTE: initBackdoor会启动http服务，但由于health check的http请求不需要追踪，且它是判断服务启动与否的关键，所以initTracer可以放在它之后进行
	xlog.Infof(ctx, "%s init backdoor start", fun)
	m.initBackdoor(sb)
	xlog.Infof(ctx, "%s init backdoor end", fun)

	xlog.Infof(ctx, "%s init handleModel start", fun)
	err = m.handleModel(sb, servLoc, args.model)
	if err != nil {
		xlog.Panicf(ctx, "%s handleModel err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init handleModel end", fun)

	xlog.Infof(ctx, "%s init dolphin start", fun)
	err = m.initDolphin(sb)
	if err != nil {
		xlog.Errorf(ctx, "%s initDolphin() failed, error: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init dolphin end", fun)

	// App层初始化
	xlog.Infof(ctx, "%s init initfn start", fun)
	err = initfn(sb)
	if err != nil {
		xlog.Panicf(ctx, "%s callInitFunc err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init initfn end", fun)

	// NOTE: processor 在初始化 trace middleware 前需要保证 xtrace.GlobalTracer() 初始化完毕
	xlog.Infof(ctx, "%s init tracer start", fun)
	m.initTracer(servLoc)
	xlog.Infof(ctx, "%s init tracer end", fun)

	xlog.Infof(ctx, "%s init processor start", fun)
	err = m.initProcessor(sb, procs, args.startType)
	if err != nil {
		xlog.Panicf(ctx, "%s initProcessor err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init processor end", fun)

	xlog.Infof(ctx, "%s init SetGroupAndDisable start", fun)
	sb.SetGroupAndDisable(args.group, args.disable)
	xlog.Infof(ctx, "%s init SetGroupAndDisable end", fun)

	xlog.Infof(ctx, "%s init metric start", fun)
	m.initMetric(sb)
	xlog.Infof(ctx, "%s init metric end", fun)

	xlog.Infof(ctx, "server start success, grpc: [%s], thrift: [%s]", GetProcessorAddress(PROCESSOR_GRPC_PROPERTY_NAME), GetProcessorAddress(PROCESSOR_THRIFT_PROPERTY_NAME))

	m.awaitSignal(sb)

	return nil
}

func parseCrossRegionIdList(idListStr string) ([]int, error) {
	if idListStr == "" {
		return nil, nil
	}

	idStrList := strings.Split(idListStr, ",")
	if len(idStrList) == 0 {
		return nil, errors.New("invalid id list string arg")
	}

	var ret []int
	for _, idStr := range idStrList {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, err
		}
		ret = append(ret, id)
	}

	return ret, nil
}

func (m *Server) awaitSignal(sb *ServBaseV2) {
	c := make(chan os.Signal, 1)
	ctx := context.Background()
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGPIPE}
	signal.Reset(signals...)
	signal.Notify(c, signals...)

	for {
		select {
		case s := <-c:
			xlog.Infof(ctx, "receive a signal:%s", s.String())

			if s.String() == syscall.SIGTERM.String() {
				xlog.Infof(ctx, "receive a signal: %s, stop server", s.String())
				sb.Stop()
				<-(chan int)(nil)
			}
		}
	}

}

func (m *Server) handleModel(sb *ServBaseV2, servLoc string, model int) error {
	fun := "Server.handleModel -->"
	ctx := context.Background()

	if model == MODEL_MASTERSLAVE {
		lockKey := fmt.Sprintf("%s-master-slave", servLoc)
		if err := sb.LockGlobal(lockKey); err != nil {
			xlog.Errorf(ctx, "%s LockGlobal key: %s, err: %v", fun, lockKey, err)
			return err
		}

		xlog.Infof(ctx, "%s LockGlobal succ, key: %s", fun, lockKey)
	}

	return nil
}

func (m *Server) initProcessor(sb *ServBaseV2, procs map[string]Processor, startType string) error {
	fun := "Server.initProcessor -->"
	ctx := context.Background()

	for n, p := range procs {
		if len(n) == 0 {
			xlog.Errorf(ctx, "%s processor name empty", fun)
			return fmt.Errorf("processor name empty")
		}

		if n[0] == '_' {
			xlog.Errorf(ctx, "%s processor name can not prefix '_'", fun)
			return fmt.Errorf("processor name can not prefix '_'")
		}

		if p == nil {
			xlog.Errorf(ctx, "%s processor:%s is nil", fun, n)
			return fmt.Errorf("processor:%s is nil", n)
		} else {
			err := p.Init()
			if err != nil {
				xlog.Errorf(ctx, "%s processor: %s init err: %v", fun, n, err)
				return fmt.Errorf("processor:%s init err:%s", n, err)
			}
		}
	}

	infos, err := m.loadDriver(procs)
	if err != nil {
		xlog.Errorf(ctx, "%s load driver err: %v", fun, err)
		return err
	}

	// 本地启动不注册至etcd
	if startType == START_TYPE_LOCAL {
		return nil
	}

	err = sb.RegisterService(infos)
	if err != nil {
		xlog.Errorf(ctx, "%s register service err: %v", fun, err)
		return err
	}

	// 注册跨机房服务
	err = sb.RegisterCrossDCService(infos)
	if err != nil {
		xlog.Errorf(ctx, "%s register cross dc failed, err: %v", fun, err)
		return err
	}

	return nil
}

func (m *Server) initTracer(servLoc string) error {
	fun := "Server.initTracer -->"
	ctx := context.Background()

	err := xtrace.InitDefaultTracer(servLoc)
	if err != nil {
		xlog.Errorf(ctx, "%s init tracer err: %v", fun, err)
	}

	err = xtrace.InitTraceSpanFilter()
	if err != nil {
		xlog.Errorf(ctx, "%s init trace span filter fail: %s", fun, err.Error())
	}

	return err
}

func (m *Server) initBackdoor(sb *ServBaseV2) error {
	fun := "Server.initBackdoor -->"
	ctx := context.Background()

	backdoor := &backDoorHttp{}
	err := backdoor.Init()
	if err != nil {
		xlog.Errorf(ctx, "%s init backdoor err: %v", fun, err)
		return err
	}

	binfos, err := m.loadDriver(map[string]Processor{"_PROC_BACKDOOR": backdoor})
	if err == nil {
		err = sb.RegisterBackDoor(binfos)
		if err != nil {
			xlog.Errorf(ctx, "%s register backdoor err: %v", fun, err)
		}

	} else {
		xlog.Warnf(ctx, "%s load backdoor driver err: %v", fun, err)
	}

	return err
}

func (m *Server) initMetric(sb *ServBaseV2) error {
	fun := "Server.initMetric -->"
	ctx := context.Background()

	metrics := xprom.NewMetricProcessor()
	err := metrics.Init()
	if err != nil {
		xlog.Warnf(ctx, "%s init metrics err: %v", fun, err)
	}

	metricInfo, err := m.loadDriver(map[string]Processor{"_PROC_METRICS": metrics})
	if err == nil {
		err = sb.RegisterMetrics(metricInfo)
		if err != nil {
			xlog.Warnf(ctx, "%s register backdoor err: %v", fun, err)
		}

	} else {
		xlog.Warnf(ctx, "%s load metrics driver err: %v", fun, err)
	}
	return err
}

func (m *Server) initDolphin(sb *ServBaseV2) error {
	fun := "Server.initDolphin -->"
	// circuit breaker
	err := circuit_breaker.Init(sb.servGroup, sb.servName)
	if err != nil {
		xlog.Errorf(context.Background(), "%s: circuit_breaker.Init() failed, error: %+v", fun, err)
		return err
	}

	// rate limiter
	etcdInterfaceRateLimitRegistry, err := registry.NewEtcdInterfaceRateLimitRegistry(sb.servGroup, sb.servName, servbase.ETCDS_CLUSTER_0)
	if err != nil {
		xlog.Errorf(context.Background(), "%s: registry.NewEtcdInterfaceRateLimitRegistry() failed, error: %+v", fun, err)
		return err
	}
	rateLimitRegistry = etcdInterfaceRateLimitRegistry
	go func() {
		etcdInterfaceRateLimitRegistry.Watch()
	}()
	return nil
}

// Serve app call Serve to start server, initLogic is the init func in app, logic.InitLogic,
func Serve(etcdAddrs []string, baseLoc string, initLogic func(ServBase) error, processors map[string]Processor) error {
	return server.Serve(configEtcd{etcdAddrs, baseLoc}, initLogic, processors)
}

// MasterSlave Leader-Follower模式，通过etcd distribute lock进行选举
func MasterSlave(etcdAddrs []string, baseLoc string, initLogic func(ServBase) error, processors map[string]Processor) error {
	return server.MasterSlave(configEtcd{etcdAddrs, baseLoc}, initLogic, processors)
}

func (m *Server) MasterSlave(confEtcd configEtcd, initLogic func(ServBase) error, processors map[string]Processor) error {
	fun := "Server.MasterSlave -->"
	ctx := context.Background()

	args, err := m.parseFlag()
	if err != nil {
		xlog.Panicf(ctx, "%s parse arg err: %v", fun, err)
		return err
	}
	args.model = MODEL_MASTERSLAVE

	return m.Init(confEtcd, args, initLogic, processors)
}

// Init use in test of application
func Init(etcdAddrs []string, baseLoc string, servLoc, servKey, logDir string, initLogic func(ServBase) error, processors map[string]Processor) error {
	args := &cmdArgs{
		logMaxSize:    0,
		logMaxBackups: 0,
		servLoc:       servLoc,
		logDir:        logDir,
		sessKey:       servKey,
	}
	return server.Init(configEtcd{etcdAddrs, baseLoc}, args, initLogic, processors)
}

func GetServBase() ServBase {
	return server.sbase
}

func GetServName() (servName string) {
	if server.sbase != nil {
		servName = server.sbase.Servname()
	}
	return
}

// GetGroupAndService return group and service name of this service
func GetGroupAndService() (group, service string) {
	serviceKey := GetServName()
	serviceKeyArray := strings.Split(serviceKey, "/")
	if len(serviceKeyArray) == 2 {
		group = serviceKeyArray[0]
		service = serviceKeyArray[1]
	}
	return
}

func GetServId() (servId int) {
	if server.sbase != nil {
		servId = server.sbase.Servid()
	}
	return
}

// GetConfigCenter get serv conf center
func GetConfigCenter() xconfig.ConfigCenter {
	if server.sbase != nil {
		return server.sbase.ConfigCenter()
	}
	return nil
}

// GetAddress get processor ip+port by processorName
func GetProcessorAddress(processorName string) (addr string) {
	ctx := context.Background()
	if server == nil {
		return
	}
	reginfos := server.sbase.RegInfos()
	for _, val := range reginfos {
		data := new(RegData)
		err := json.Unmarshal([]byte(val), data)
		if err != nil {
			xlog.Warnf(ctx, "GetProcessorAddress unmarshal, val = %s, err = %s", val, err.Error())
			continue
		}
		if servInfo, ok := data.Servs[processorName]; ok {
			addr = servInfo.Addr
			return
		}
	}
	return
}

// Test 方便开发人员在本地启动服务、测试，实例信息不会注册到etcd
func Test(etcdAddrs []string, baseLoc, servLoc string, initLogic func(ServBase) error) error {
	args := &cmdArgs{
		logMaxSize:    0,
		logMaxBackups: 0,
		servLoc:       servLoc,
		sessKey:       "test",
		logDir:        "console",
		disable:       true,
	}
	return server.Init(configEtcd{etcdAddrs, baseLoc}, args, initLogic, nil)
}
