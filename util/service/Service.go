// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rocserv

import (
	"flag"
	"fmt"
	"github.com/shawnfeng/roc/util/service/sla"
	"github.com/shawnfeng/sutil/trace"
	"reflect"

	"github.com/julienschmidt/httprouter"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/slog/statlog"
)

const (
	PROCESSOR_HTTP   = "http"
	PROCESSOR_THRIFT = "thrift"
	PROCESSOR_GRPC   = "gprc"
)

type Service struct {
}

var service Service

//func NewService

type cmdArgs struct {
	servLoc string
	logDir  string
	sessKey string
	group   string
}

func (m *Service) parseFlag() (*cmdArgs, error) {
	var serv, logDir, skey, group string
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
		servLoc: serv,
		logDir:  logDir,
		sessKey: skey,
		group:   group,
	}, nil

}

func (m *Service) loadDriver(sb ServBase, procs map[string]Processor) (map[string]*ServInfo, error) {
	fun := "Service.loadDriver -->"

	infos := make(map[string]*ServInfo)

	// load processor's driver
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
		default:
			return nil, fmt.Errorf("processor:%s driver not recognition", n)

		}

	}

	return infos, nil

}

func (m *Service) Serve(confEtcd configEtcd, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Service.Serve -->"

	args, err := m.parseFlag()
	if err != nil {
		slog.Panicf("%s parse arg err:%s", fun, err)
		return err
	}

	return m.Init(confEtcd, args.servLoc, args.sessKey, args.logDir, args.group, initfn, procs)
}

func (m *Service) Init(confEtcd configEtcd, servLoc, sessKey, logDir, group string, initfn func(ServBase) error, procs map[string]Processor) error {
	fun := "Service.Init -->"

	sb, err := NewServBaseV2(confEtcd, servLoc, sessKey)
	if err != nil {
		slog.Panicf("%s init servbase loc:%s key:%s err:%s", fun, servLoc, sessKey, err)
		return err
	}

	var logConfig struct {
		Log struct {
			Level string
			Dir   string
		}
	}
	// default use level INFO
	logConfig.Log.Level = "INFO"

	err = sb.ServConfig(&logConfig)
	if err != nil {
		slog.Panicf("%s serv config err:%s", fun, err)
		fmt.Sprintf("%s serv config err:%s", fun, err)
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

	fmt.Sprintf("%s init log dir:%s name:%s level:%s", fun, logdir, servLoc, logConfig.Log.Level)
	slog.Infof("%s init log dir:%s name:%s level:%s", fun, logdir, servLoc, logConfig.Log.Level)

	slog.Init(logdir, "serv.log", logConfig.Log.Level)
	defer slog.Sync()

	statlog.Init(logdir, "stat.log", servLoc)
	defer statlog.Sync()

	// init tracer
	err = trace.InitDefaultTracer(servLoc)
	if err != nil {
		slog.Warnf("%s init tracer fail:%v", err)
	}

	// init callback
	err = initfn(sb)
	if err != nil {
		slog.Panicf("%s serv init err:%s", fun, err)
		return err
	}

	// init processor
	for n, p := range procs {
		if len(n) == 0 {
			slog.Panicf("%s processor name empty", fun)
			return err
		}

		if n[0] == '_' {
			slog.Panicf("%s processor name can not prefix '_'", fun)
			return err
		}

		if p == nil {
			slog.Panicf("%s processor:%s is nil", fun, n)
			return fmt.Errorf("processor:%s is nil", n)
		} else {
			err := p.Init()
			if err != nil {
				slog.Panicf("%s processor:%s init err:%s", fun, err)
				return err
			}
		}
	}

	infos, err := m.loadDriver(sb, procs)
	if err != nil {
		slog.Panicf("%s load driver err:%s", fun, err)
		return err
	}

	err = sb.RegisterService(infos)
	if err != nil {
		slog.Panicf("%s regist service err:%s", fun, err)
		return err
	}

	sb.SetGroup(group)

	// 后门接口 ==================
	backdoor := &backDoorHttp{}
	err = backdoor.Init()
	if err != nil {
		slog.Panicf("%s init backdoor err:%s", fun, err)
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
	//==============================

	// sla metric埋点 ==================
	//init metric
	//user defualt metric opts
	metrics := rocserv.NewMetricsprocessor()
	if err != nil {
		slog.Warnf("init metrics fail:%v", err)
	}
	err = metrics.Init()
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
	//==============================

	// pause here
	var pause chan bool
	pause <- true

	return nil

}
func (m *Service) getMetricOps(sb *ServBaseV2) *rocserv.MetricsOpts {
	fun := "Service.getMetricOps -->"
	var metricConfig struct {
		metric *rocserv.MetricsOpts
	}
	err := sb.ServConfig(&metricConfig)
	if err != nil {
		slog.Panicf("%s serv config err:%s", fun, err)
		fmt.Sprintf("%s serv config err:%s", fun, err)
		return nil
	}
	return metricConfig.metric
}

func Serve(etcds []string, baseLoc string, initfn func(ServBase) error, procs map[string]Processor) error {
	return service.Serve(configEtcd{etcds, baseLoc}, initfn, procs)
}

func Init(etcds []string, baseLoc string, servLoc, servKey string, initfn func(ServBase) error, procs map[string]Processor) error {
	return service.Init(configEtcd{etcds, baseLoc}, servLoc, servKey, "", "", initfn, procs)
}

func Test(etcds []string, baseLoc string, initfn func(ServBase) error) error {
	return service.Init(configEtcd{etcds, baseLoc}, "test/test", "test", "console", "", initfn, nil)
}
