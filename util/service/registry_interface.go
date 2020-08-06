package rocserv

type ClientLookup interface {
	GetServAddr(processor, key string) *ServInfo
	GetServAddrWithServid(servid int, processor, key string) *ServInfo
	GetServAddrWithGroup(group string, processor, key string) *ServInfo
	GetAllServAddr(processor string) []*ServInfo
	GetAllServAddrWithGroup(group, processor string) []*ServInfo
	ServKey() string
	ServPath() string
}

func NewClientLookup(etcdaddrs []string, baseLoc string, servlocation string) (*ClientEtcdV2, error) {
	return NewClientEtcdV2(configEtcd{etcdaddrs, baseLoc}, servlocation)
}
