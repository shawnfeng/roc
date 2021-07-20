package rocserv

import (
	"context"
	"errors"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xconfig"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/util/servbase"
)

/*
 * roc 旧的入口方法为 `Serve()`，而且在其中进入了非常多的初始化逻辑(见 `initServer()` 方法)，比如初始化 config center 之类。
 * 这种设计理念，暗含的假设是「将 roc 作为唯一的执行入口」。
 * 对于 RPC 服务来说，这个假设关系不大。但现在我们要支持非 RPC，比如 job，这个假设就不成立了。
 * 因此，此 V2 版另外提供了 `Start()` 作为入口方法。其仅完成 roc 本身必需的功能。
 */

// RocOptions roc 启动参数
type RocOptions struct {
	etcdAddrs []string    // 进行服务注册的 etcd 集群地址
	baseLoc   string      // 进行服务注册的 etcd base 路径
	model     ServerModel // 运行模式

	disable bool // 是否禁用。从 cmdArgs 中迁出来的参数。似已无用，待删除。

	args                      *cmdArgs // 命令行参数
	configCenter              xconfig.ConfigCenter
	createConfigCenterWhenNil bool // 如果 configCenter 为 nil，则创建之。V1 须置为 true，V2 须置为 false。
	procs                     map[string]Processor
}

func DefaultRocOptions() *RocOptions {
	return &RocOptions{
		etcdAddrs: servbase.ETCDS_CLUSTER_0,
		baseLoc:   servbase.ETCD_BASE_LOCTION_SERV,
		model:     MODEL_SERVER,
	}
}

type Option func(*RocOptions)

// EtcdAddrs 设置用于服务注册的 etcd 地址
func EtcdAddrs(addrs []string) Option {
	return func(options *RocOptions) {
		options.etcdAddrs = addrs
	}
}

// BaseLoc 设置服务注册时的根路径
func BaseLoc(baseLoc string) Option {
	return func(options *RocOptions) {
		options.baseLoc = baseLoc
	}
}

// RunModel 用于兼容之前的 Master/Slave 运行模式
func RunModel(m ServerModel) Option {
	return func(options *RocOptions) {
		options.model = m
	}
}

func Start(configCenter xconfig.ConfigCenter, procs map[string]Processor, opts ...Option) error {
	return server.Start(configCenter, procs, opts...)
}

func (m *Server) Start(configCenter xconfig.ConfigCenter, procs map[string]Processor, opts ...Option) error {
	ctx := context.Background()
	fun := "Server.Start -->"

	if configCenter == nil {
		xlog.Errorf(context.Background(), "%s configCenter should NOT be nil!", fun)
		return errors.New("configCenter should NOT be nil")
	}

	options := DefaultRocOptions()
	options.configCenter = configCenter
	options.procs = procs

	args, err := m.parseFlag()
	if err != nil {
		xlog.Errorf(ctx, "%s parse arg err: %v", fun, err)
		return err
	}
	options.args = args

	for _, opt := range opts {
		opt(options)
	}

	awaitSignal := true
	return m.InitV2(options, awaitSignal)
}

func (m *Server) InitV2(options *RocOptions, awaitSignal bool) error {
	err := m.initServerV2(options)
	if err != nil {
		return err
	}

	if awaitSignal {
		m.awaitSignal(m.sbase)
	}
	return nil
}

func (m *Server) initServerV2(options *RocOptions) error {
	// 相较于 V1 版的 initServer() 方法，此方法去掉了以下行为:
	// initLog()
	// stat.Init()
	// initDolphin(): V2 不再初始化 circuit breaker，而只初始化 rate limiter
	// initfn()
	// initTracer()
	// 以上这些，V2 都应在业务代码中，在运行 roc 之前进行完毕
	fun := "Server.initServerV2 -->"
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
	err = m.initDolphin(sb, false)
	if err != nil {
		xlog.Errorf(ctx, "%s initDolphin() failed, error: %v", fun, err)
		return err
	}
	xlog.Infof(ctx, "%s init dolphin end", fun)

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

// TestV2 TODO: 换个更好的名字？
func TestV2(configCenter xconfig.ConfigCenter, servLoc string, opts ...Option) error {
	fun := "TestV2 -->"

	if configCenter == nil {
		xlog.Errorf(context.Background(), "%s configCenter should NOT be nil!", fun)
		return errors.New("configCenter should NOT be nil")
	}

	args := &cmdArgs{
		servLoc:   servLoc,
		sessKey:   "test",
		logDir:    "console",
		startType: START_TYPE_LOCAL,
	}

	options := DefaultRocOptions()
	options.disable = true
	options.configCenter = configCenter
	options.args = args
	for _, opt := range opts {
		opt(options)
	}

	return server.InitV2(options, false)
}
