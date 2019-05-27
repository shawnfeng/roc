namespace go goodbye

include "ThriftUtil.thrift"

service GoodbyeService {
    string SayGoodbye(1:string name, 10:ThriftUtil.Context ctx)
}