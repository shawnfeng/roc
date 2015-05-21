// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package rocserv

import (
    "flag"
	"fmt"
    "reflect"

    "github.com/julienschmidt/httprouter"

	"git.apache.org/thrift.git/lib/go/thrift"


	"github.com/shawnfeng/sutil/slog"

)

const (
	PROCESSOR_HTTP = "http"
	PROCESSOR_THRIFT = "thrift"
)





type Service struct {


}

var service Service

//func NewService

type cmdArgs struct {
	servLoc string
	//logDir   string
	sessKey  string     

}

func (m *Service) parseFlag() (*cmdArgs, error) {
	var serv, skey string
    flag.StringVar(&serv, "serv", "", "servic name")
	//flag.StringVar(&logdir, "log", "", "serice log dir")
	flag.StringVar(&skey, "skey", "", "service session key")
 
    flag.Parse()

	if len(serv) == 0 {
		return nil, fmt.Errorf("serv args need!")
	}

	/*
	if len(logdir) == 0 {
		return nil, fmt.Errorf("log args need!")
	}
   */

	if len(skey) == 0 {
		return nil, fmt.Errorf("skey args need!")
	}


	return &cmdArgs {
		servLoc: serv,
		//logDir: logdir,
		sessKey: skey,
	}, nil

}


func (m *Service) loadDriver(sb ServBase, procs map[string]Processor) error {
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
				return err
			}

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo {
				Type: PROCESSOR_HTTP,
				Addr: sa,
			}


		case thrift.TProcessor:
			sa, err := powerThrift(addr, d)
			if err != nil {
				return err
			}

			slog.Infof("%s load ok processor:%s serv addr:%s", fun, n, sa)
			infos[n] = &ServInfo {
				Type: PROCESSOR_THRIFT,
				Addr: sa,
			}


		default:
			return fmt.Errorf("processor:%s driver not recognition", n)

		}

	}

	return sb.RegisterService(infos)


}


func (m *Service) Serve(etcds []string, initfn func (ServBase) error, procs map[string]Processor) error {
	fun := "Service.Serve -->"

	args, err := m.parseFlag()
	if err != nil {
		slog.Panicf("%s parse arg err:%s", fun, err)
		return err
	}


	// Init ServBase

	sb, err := NewServBaseV2(etcds, args.servLoc, args.sessKey)
	if err != nil {
		slog.Panicf("%s init servbase args:%s err:%s", fun, args, err)
		return err
	}


	// Init slog
	var logConfig struct {
		Log struct {
			Level string
			Dir string
		}
	}
	// default use level INFO
	logConfig.Log.Level = "INFO"

	err = sb.ServConfig(&logConfig)
	if err != nil {
		slog.Panicf("%s serv config err:%s", fun, err)
		return err
	}

	var logdir string
	if len(logConfig.Log.Dir) > 0 {
		logdir = fmt.Sprintf("%s/%s", logConfig.Log.Dir, sb.Copyname())
	}

	slog.Infof("%s init log dir:%s name:%s level:%s", fun, logdir, args.servLoc, logConfig.Log.Level)

	slog.Init(logdir, "serv", logConfig.Log.Level)


	// init callback
	err = initfn(sb)
	if err != nil {
		slog.Panicf("%s serv init err:%s", fun, err)
		return err
	}


	// init processor
	for n, p := range procs {
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


	if err := m.loadDriver(sb, procs); err != nil {
		slog.Panicf("%s load driver err:%s", fun, err)
		return err
	}


	// pause here
	var pause chan bool
	pause <- true

	return nil

}



func Serve(etcds []string, initfn func (ServBase) error, procs map[string]Processor) error {
	return service.Serve(etcds, initfn, procs)
}
