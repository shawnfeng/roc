package main

import (
	//"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/shawnfeng/sutil/slog"

	. "github.com/shawnfeng/roc/util/service"

	"github.com/shawnfeng/roc/util/service/test/idl/gen-go/demo/rpc"
)

type RpcServiceImpl struct {
}

func (this *RpcServiceImpl) FunCall(callTime int64, funCode string, paramMap map[string]string) (r []string, err error) {
	slog.Infoln("-->FunCall:", callTime, funCode, paramMap)

	for k, v := range paramMap {
		r = append(r, k+v)
	}
	return
}
// ========================================

type ProcHttp struct {


}

func (m *ProcHttp) Init(sb ServBase) error {

	slog.Infoln("Hello Init ProcHttp", sb.Copyname())

	return nil
}

func (m *ProcHttp) Driver() (string, interface{}) {


	slog.Infoln("Driver Location http")

	return "192.168.1.198:", nil

}


type ProcThrift struct {


}

func (m *ProcThrift) Init(sb ServBase) error {

	slog.Infoln("Hello Init ProcThrift", sb.Copyname())

	return nil
}

func (m *ProcThrift) Driver() (string, interface{}) {


	slog.Infoln("Driver Location sb")

	handler := &RpcServiceImpl{}
	processor := rpc.NewRpcServiceProcessor(handler)


	return ":", processor

}




// ./ServTest  -etcd http://127.0.0.1:20002,http://127.0.0.1:20003 -serv fuck -buss niubi -log ./log -skey aaasdfasdfa
func main() {

	ps := map[string]Processor {
		"testHttp": &ProcHttp{},
		"testThrift": &ProcThrift{},
	}

	err := Serve(ps)
	if err != nil {
		slog.Errorf("serve err:%s", err)
	}

}


