module github.com/shawnfeng/roc

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/gin-gonic/gin v1.4.0
	github.com/golangci/golangci-lint v1.31.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/julienschmidt/httprouter v1.2.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/sdming/gosnow v0.0.0-20130403030620-3a05c415e886
	github.com/shawnfeng/consistent v1.0.3
	github.com/shawnfeng/hystrix-go v0.0.0-20190320120533-5e2bc39f173a
	github.com/shawnfeng/sutil v1.4.11
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	gitlab.pri.ibanyu.com/middleware/dolphin v1.0.6
	gitlab.pri.ibanyu.com/middleware/seaweed v1.2.51
	gitlab.pri.ibanyu.com/middleware/util v1.3.93
	gitlab.pri.ibanyu.com/server/servmonitor/pub.git v0.0.0-20201126101549-2540f6a10b42
	gitlab.pri.ibanyu.com/tracing/go-grpc v0.0.0-20201117083632-fd2d4bfc37a7
	gitlab.pri.ibanyu.com/tracing/go-stdlib v1.0.1-0.20201126030004-a3785d4be9ed
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
	google.golang.org/grpc v1.24.0
)

go 1.13
