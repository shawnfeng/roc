module github.com/shawnfeng/roc

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5 // indirect
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/gin-gonic/gin v1.4.0
	// nn
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	// nn
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/julienschmidt/httprouter v1.0.1-0.20150106073633-b55664b9e920
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.1.0
	github.com/prometheus/client_golang v0.9.2
	github.com/sdming/gosnow v0.0.0-20130403030620-3a05c415e886
	github.com/shawnfeng/consistent v1.0.3
	github.com/shawnfeng/dbrouter v1.0.2
	github.com/shawnfeng/hystrix-go v0.0.0-20190320120533-5e2bc39f173a
	github.com/shawnfeng/sutil v1.0.6-0.20190704114539-9ede5164c60b

	// nn
	github.com/smartystreets/assertions v0.0.0-20180927180507-b2de0cb4f26d // indirect
	// nn
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	// 这个库，老的依赖拷贝没有.git目录，不知道对应哪个版本，这个就用最新的吧
	// 官方的库应该问题不大

	golang.org/x/net v0.0.0-20190522155817-f3200d17e092
	google.golang.org/grpc v1.20.0

)
