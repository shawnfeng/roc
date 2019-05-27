namespace go hello

include "ThriftUtil.thrift"

service HelloService {
    string SayHello(1:string name, 10:ThriftUtil.Context ctx)
}