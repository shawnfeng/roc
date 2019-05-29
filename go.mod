module github.com/shawnfeng/roc

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5 // indirect
	github.com/coreos/etcd v3.0.0-beta.0.0.20160712024141-cc26f2c8892e+incompatible
	// nn
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	// nn
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/julienschmidt/httprouter v1.0.1-0.20150106073633-b55664b9e920
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02 // indirect
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9 // indirect
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/prometheus/client_golang v0.9.2
	github.com/sdming/gosnow v0.0.0-20130403030620-3a05c415e886
	github.com/shawnfeng/consistent v1.0.3
	github.com/shawnfeng/dbrouter v1.0.2
	github.com/shawnfeng/hystrix-go v0.0.0-20190320120533-5e2bc39f173a
	github.com/shawnfeng/sutil v1.0.5
	// nn
	github.com/smartystreets/assertions v0.0.0-20180927180507-b2de0cb4f26d // indirect
	// nn
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	github.com/ugorji/go v0.0.0-20160531122944-b94837a2404a // indirect
	// 这个库，老的依赖拷贝没有.git目录，不知道对应哪个版本，这个就用最新的吧
	// 官方的库应该问题不大
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	google.golang.org/grpc v1.20.0

)
