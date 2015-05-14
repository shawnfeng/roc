package rocserv

import (
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/shawnfeng/sutil/slog"
)


type ClientLookup interface {

	GetServAddr(processor, key string) *ServInfo

}



func NewClientLookup(etcdaddrs[]string, servlocation, servname string) (*ClientEtcdV2, error) {
	return NewClientEtcdV2(etcdaddrs, servlocation, servname)
}



type ClientThrift struct {

	clientLookup ClientLookup
	processor string
	fnFactory func(thrift.TTransport, thrift.TProtocolFactory) interface{}

	poolLen int



	muPool sync.Mutex
	poolClient map[string]chan interface{}

}



func NewClientThrift(cb ClientLookup, processor string, fn func(thrift.TTransport, thrift.TProtocolFactory) interface{}, poollen int) *ClientThrift {

	ct := &ClientThrift {
		clientLookup: cb,
		processor: processor,
		fnFactory: fn,
		poolLen: poollen,
		poolClient: make(map[string]chan interface{}),
	}

	return ct
}

func (m *ClientThrift) newClient(addr string) interface{} {
	fun := "ClientThrift.newClient -->"

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		slog.Errorf("%s NetTSocket addr:%s err:%s", fun, addr, err)
		return nil
	}
	useTransport := transportFactory.GetTransport(transport)

	if err := useTransport.Open(); err != nil {
		slog.Errorf("%s Open addr:%s err:%s", fun, addr, err)
		return nil
	}
	// 必须要close么？
	//useTransport.Close()

	slog.Infof("%s new client addr:%s", fun, addr)
	return m.fnFactory(useTransport, protocolFactory)

}

func (m *ClientThrift) getPool(addr string) chan interface{} {
	m.muPool.Lock()
	defer m.muPool.Unlock()
	tmp, ok := m.poolClient[addr]
	if !ok {
		tmp = make(chan interface{}, m.poolLen)
		m.poolClient[addr] = tmp

	}
	return tmp

}

// just use debug
func (m *ClientThrift) printPool() {
	fun := "ClientThrift.printPool -->"
	m.muPool.Lock()
	defer m.muPool.Unlock()

	slog.Debugf("%s len:%d pool:%s", fun, len(m.poolClient), m.poolClient)
}


func (m *ClientThrift) Get(key string) (*ServInfo, interface{}) {
	fun := "ClientThrift.Get -->"

	s := m.clientLookup.GetServAddr(m.processor, key)
	if s == nil {
		return nil, nil
	}

	addr := s.Addr


	po := m.getPool(addr)

	var c interface{}
	select {
	case c = <-po:
		slog.Tracef("%s get:%s len:%d", fun, addr, len(po))
	default:
		c = m.newClient(addr)
	}

	//m.printPool()
	return s, c
}



func (m *ClientThrift) Payback(si *ServInfo, client interface{}) {
	fun := "ClientThrift.Payback -->"

	po := m.getPool(si.Addr)

	select {
	case po <- client:
		slog.Tracef("%s payback:%s len:%d", fun, si, len(po))
	default:
		slog.Infof("%s full not payback:%s len:%d", fun, si, len(po))
	}

	//m.printPool()
}
