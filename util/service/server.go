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
type ServerModel int

const (
	MODEL_SERVER      ServerModel = 0
	MODEL_MASTERSLAVE ServerModel = 1
)

const START_TYPE_LOCAL = "local"

const (
	// ENV_LANE 环境变量用于指定「泳道」。
	ENV_LANE = "LANE"
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
	servLoc           string
	logDir            string
	sessKey           string
	group             string
	startType         string // 启动方式：local - 不注册至etcd
	crossRegionIdList string
	region            string
	backdoorPort      string
}

func (m *Server) parseFlag() (*cmdArgs, error) {
	const GroupUnset = "UNSET" // palceholder, 用于标记「group 未设置」
	var serv, logDir, skey, group, startType, backdoorPort string
	flag.StringVar(&serv, "serv", "", "servic name")
	flag.StringVar(&logDir, "logdir", "", "serice log dir")
	flag.StringVar(&skey, "skey", "", "service session key")
	flag.StringVar(&group, "group", GroupUnset, "泳道。已废弃，请使用环境变量 LANE 设置泳道")
	// 启动方式：local - 不注册至etcd
	flag.StringVar(&startType, "stype", "", "start up type, local is not register to etcd")
	flag.StringVar(&backdoorPort, "backdoor_port", "", "service backdoor port")

	flag.Parse()

	// 优先启动参数
	if backdoorPort == "" {
		backdoorPort = os.Getenv("BACKDOORPORT")
	}

	if len(serv) == 0 {
		return nil, fmt.Errorf("serv args need!")
	}

	if len(skey) == 0 {
		return nil, fmt.Errorf("skey args need!")
	}

	crossRegionIdList := os.Getenv("CROSSREGIONIDLIST")

	region := getRegionFromEnvOrDefault()

	// 优先级: 命令行最高，次之环境变量
	if group == GroupUnset {
		group = os.Getenv(ENV_LANE)
	}

	return &cmdArgs{
		servLoc:           serv,
		logDir:            logDir,
		sessKey:           skey,
		group:             group,
		startType:         startType,
		crossRegionIdList: crossRegionIdList,
		region:            region,
		backdoorPort:      backdoorPort,
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
func (m *Server) Serve(confEtcd configEtcd, initfn func(ServBase) error, procs map[string]Processor, model ServerModel) error {
	fun := "Server.Serve -->"

	options := DefaultRocOptions()
	options.etcdAddrs = confEtcd.etcdAddrs
	options.baseLoc = confEtcd.useBaseloc
	options.model = model
	options.createConfigCenterWhenNil = true
	options.procs = procs

	args, err := m.parseFlag()
	if err != nil {
		xlog.Errorf(context.Background(), "%s parse arg err: %v", fun, err)
		return err
	}
	options.args = args

	return m.Init(options, initfn, true)
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
	extraHeaders := map[string]interface{}{
		"region": sb.Region(),
		"lane":   sb.Lane(),
		"ip":     sb.ServIp(),
	}
	xlog.InitAppLogV2(logdir, "serv.log", convertLevel(logConfig.Log.Level), extraHeaders)
	xlog.InitStatLog(logdir, "stat.log")
	xlog.SetStatLogService(args.servLoc)
	return nil
}

// 	func (m *Server) Init(confEtcd configEtcd, args *cmdArgs, initfn func(ServBase) error, procs map[string]Processor, awaitSignal bool) error {...}
func (m *Server) Init(options *RocOptions, initfn func(ServBase) error, awaitSignal bool) error {
	fun := "Server.Init -->"

	if err := m.initServer(options, initfn); err != nil {
		return err
	}

	if awaitSignal {
		xlog.Infof(context.Background(), "%s awaiting signal", fun)
		m.awaitSignal(m.sbase)
	}
	return nil
}

func (m *Server) initServer(options *RocOptions, initfn func(ServBase) error) error {
	fun := "Server.initServer -->"
	ctx := context.Background()

	xlog.Infof(ctx, "%s new ServBaseV2 start", fun)
	sb, err := newServBaseV2WithOptions(options)
	if err != nil {
		xlog.Errorf(ctx, "%s init servbase loc: %s key: %s err: %v", fun, options.args.servLoc, options.args.sessKey, err)
		return err
	}
	m.sbase = sb
	xlog.Infof(ctx, "%s new ServBaseV2 end", fun)

	//将ip存储
	if err := sb.setIp(); err != nil {
		xlog.Errorf(ctx, "%s set ip error: %v", fun, err)
	}

	// 初始化日志
	xlog.Infof(ctx, "%s initLog start", fun)
	m.initLog(sb, options.args)
	xlog.Infof(ctx, "%s initLog end", fun)

	// 初始化服务进程打点
	xlog.Infof(ctx, "%s init stat start", fun)
	stat.Init(sb.servGroup, sb.servName, "")
	xlog.Infof(ctx, "%s init stat end", fun)

	defer xlog.AppLogSync()
	defer xlog.StatLogSync()

	// NOTE: initBackdoor会启动http服务，但由于health check的http请求不需要追踪，且它是判断服务启动与否的关键，所以initTracer可以放在它之后进行
	xlog.Infof(ctx, "%s init backdoor start", fun)
	m.initBackdoor(sb, options.args)
	xlog.Infof(ctx, "%s init backdoor end", fun)

	xlog.Infof(ctx, "%s init handleModel start", fun)
	err = m.handleModel(sb, options.args.servLoc, options.model)
	if err != nil {
		xlog.Errorf(ctx, "%s handleModel err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init handleModel end", fun)

	xlog.Infof(ctx, "%s init dolphin start", fun)
	err = m.initDolphin(sb, true)
	if err != nil {
		xlog.Errorf(ctx, "%s initDolphin() failed, error: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init dolphin end", fun)

	// App层初始化
	xlog.Infof(ctx, "%s init initfn start", fun)
	err = initfn(sb)
	if err != nil {
		xlog.Errorf(ctx, "%s callInitFunc err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init initfn end", fun)

	// NOTE: processor 在初始化 trace middleware 前需要保证 xtrace.GlobalTracer() 初始化完毕
	xlog.Infof(ctx, "%s init tracer start", fun)
	m.initTracer(options.args.servLoc)
	xlog.Infof(ctx, "%s init tracer end", fun)

	xlog.Infof(ctx, "%s init processor start", fun)
	err = m.initProcessor(sb, options.procs, options.args.startType)
	if err != nil {
		xlog.Errorf(ctx, "%s initProcessor err: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init processor end", fun)

	xlog.Infof(ctx, "%s init SetGroupAndDisable start", fun)
	sb.SetGroupAndDisable(options.args.group, options.disable)
	xlog.Infof(ctx, "%s init SetGroupAndDisable end", fun)

	xlog.Infof(ctx, "%s init metric start", fun)
	m.initMetric(sb)
	xlog.Infof(ctx, "%s init metric end", fun)

	xlog.Infof(ctx, "server start success, grpc: [%s], thrift: [%s]", GetProcessorAddress(PROCESSOR_GRPC_PROPERTY_NAME), GetProcessorAddress(PROCESSOR_THRIFT_PROPERTY_NAME))
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

func (m *Server) awaitSignal(sb ServBase) {
	c := make(chan os.Signal, 1)
	ctx := context.Background()
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGPIPE}
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

func (m *Server) handleModel(sb *ServBaseV2, servLoc string, model ServerModel) error {
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
	if sb.IsLocalRunning() {
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

func (m *Server) initBackdoor(sb *ServBaseV2, args *cmdArgs) error {
	fun := "Server.initBackdoor -->"
	ctx := context.Background()

	backdoor := &backDoorHttp{
		port: args.backdoorPort,
	}
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

func (m *Server) initDolphin(sb *ServBaseV2, includeCircuitBreaker bool) error {
	// V1 版需要在 roc 中初始化 circuit breaker，但 V2 版则需要在 roc 外部初始化 circuit breaker。
	fun := "Server.initDolphin -->"

	if includeCircuitBreaker {
		// circuit breaker
		err := circuit_breaker.Init(sb.servGroup, sb.servName)
		if err != nil {
			xlog.Errorf(context.Background(), "%s: circuit_breaker.Init() failed, error: %+v", fun, err)
			return err
		}
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
	return server.Serve(configEtcd{etcdAddrs, baseLoc}, initLogic, processors, MODEL_SERVER)
}

// MasterSlave Leader-Follower模式，通过etcd distribute lock进行选举
func MasterSlave(etcdAddrs []string, baseLoc string, initLogic func(ServBase) error, processors map[string]Processor) error {
	return server.Serve(configEtcd{etcdAddrs, baseLoc}, initLogic, processors, MODEL_MASTERSLAVE)
}

// Init use in test of application
// Deprecated, use Test instead
func Init(etcdAddrs []string, baseLoc string, servLoc, servKey, logDir string, initLogic func(ServBase) error, processors map[string]Processor) error {
	options := &RocOptions{
		etcdAddrs: etcdAddrs,
		baseLoc:   baseLoc,
		args: &cmdArgs{
			servLoc: servLoc,
			logDir:  logDir,
			sessKey: servKey,
		},
		createConfigCenterWhenNil: true,
		procs:                     processors,
	}

	return server.Init(options, initLogic, true)
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

func getRegionFromEnvOrDefault() string {
	region := os.Getenv("REGION")
	if region == "" {
		region = "cn"
	}

	return strings.ToLower(region)
}

// Test 方便开发人员在本地启动服务、测试，实例信息不会注册到etcd
func Test(etcdAddrs []string, baseLoc, servLoc string, initLogic func(ServBase) error) error {
	options := &RocOptions{
		etcdAddrs: etcdAddrs,
		baseLoc:   baseLoc,
		disable:   true,
		args: &cmdArgs{
			servLoc:   servLoc,
			sessKey:   "test",
			logDir:    "console",
			startType: START_TYPE_LOCAL,
		},
		createConfigCenterWhenNil: true,
	}

	return server.Init(options, initLogic, true)
}

// 用于测试时启动框架, 功能同Test(), 但启动完成后不会阻塞
func ServeForTest(etcdAddrs []string, baseLoc, servLoc string, initLogic func(ServBase) error) error {
	options := &RocOptions{
		etcdAddrs: etcdAddrs,
		baseLoc:   baseLoc,
		disable:   true,
		args: &cmdArgs{
			servLoc:   servLoc,
			sessKey:   "test",
			logDir:    "console",
			startType: START_TYPE_LOCAL,
		},
		createConfigCenterWhenNil: true,
	}

	return server.Init(options, initLogic, false)
}
