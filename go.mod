module github.com/shawnfeng/roc

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/HdrHistogram/hdrhistogram-go v1.0.0 // indirect
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/gin-gonic/gin v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/julienschmidt/httprouter v1.2.0
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/shawnfeng/consistent v1.0.3
	github.com/stretchr/testify v1.6.1
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	gitlab.pri.ibanyu.com/middleware/dolphin v1.0.6
	gitlab.pri.ibanyu.com/middleware/seaweed v1.2.24-0.20201116103830-57469ae7cc8a
	gitlab.pri.ibanyu.com/middleware/util v1.2.21-0.20201112030807-67e99b989e3b
	gitlab.pri.ibanyu.com/server/servmonitor/pub.git v0.0.0-20201104035512-0152ae98fa6a
	gitlab.pri.ibanyu.com/tracing/go-stdlib v1.0.1-0.20201116034204-1c212862c5cb
	google.golang.org/grpc v1.24.0
)

go 1.13
