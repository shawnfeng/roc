module github.com/shawnfeng/roc

replace code.ibanyu.com/server/go/util.git => /Users/hezheng/zhenghe/banyu/server/go/util

require (
	code.ibanyu.com/server/go/util.git v0.0.0-20190525075929-924e45390c15
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/etcd v3.3.2+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/golang/protobuf v1.3.1
	github.com/julienschmidt/httprouter v1.0.1-0.20150106073633-b55664b9e920
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.1.0
	github.com/prometheus/client_golang v0.9.2
	github.com/sdming/gosnow v0.0.0-20130403030620-3a05c415e886
	github.com/shawnfeng/consistent v1.0.3
	github.com/shawnfeng/dbrouter v1.0.3-0.20190227065045-efab6e6f8826
	github.com/shawnfeng/hystrix-go v0.0.0-20190320120533-5e2bc39f173a
	github.com/shawnfeng/sutil v1.0.5
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/ugorji/go v1.1.1 // indirect
	// 这个库，老的依赖拷贝没有.git目录，不知道对应哪个版本，这个就用最新的吧
	// 官方的库应该问题不大
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	google.golang.org/grpc v1.20.0

)
