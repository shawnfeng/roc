module github.com/shawnfeng/roc

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/gin-gonic/gin v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/julienschmidt/httprouter v1.2.0
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/shawnfeng/consistent v1.0.3
	github.com/shawnfeng/hystrix-go v0.0.0-20190320120533-5e2bc39f173a
	github.com/stretchr/testify v1.6.1
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	gitlab.pri.ibanyu.com/middleware/dolphin v1.0.6
	gitlab.pri.ibanyu.com/middleware/seaweed v1.2.18
	gitlab.pri.ibanyu.com/middleware/util v1.2.1
	google.golang.org/grpc v1.24.0
)

go 1.13
