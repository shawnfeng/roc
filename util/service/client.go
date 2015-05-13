package rocserv


type ClientBase interface {

	GetServAddr(key string) *ServInfo

}



func NewClientEtcd(etcdaddrs[]string, servlocation, servname string) (*ClientEtcdV2, error) {

	return NewClientEtcdV2(etcdaddrs, servlocation, servname)

}

