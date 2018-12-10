module github.com/shawnfeng/roc

replace (
	code.google.com/p/go-uuid => github.com/shawnfeng/googleuuid v1.0.0
	code.google.com/p/goprotobuf => github.com/shawnfeng/googlpb v1.0.0
)

require (
	git.apache.org/thrift.git v0.0.0-20150427210205-dc799ca07862
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5
	github.com/coreos/etcd v3.0.0-beta.0.0.20160712024141-cc26f2c8892e+incompatible

	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/jmoiron/sqlx v1.2.0 // indirect
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/julienschmidt/httprouter v1.2.0
	github.com/sdming/gosnow v0.0.0-20130403030620-3a05c415e886
	github.com/shawnfeng/consistent v0.0.0-20160720050957-8a5abdf8c1be
	github.com/shawnfeng/dbrouter v1.0.0
	github.com/shawnfeng/sutil v1.0.0
	github.com/smartystreets/assertions v0.0.0-20180927180507-b2de0cb4f26d // indirect
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	github.com/ugorji/go/codec v0.0.0-20181209151446-772ced7fd4c2 // indirect
	golang.org/x/net v0.0.0-20181207154023-610586996380
	google.golang.org/appengine v1.1.0 // indirect
)
