package http

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/opentracing/opentracing-go"
	"github.com/shawnfeng/roc/util/service/tracingtest/adapter/grpc"
	"github.com/shawnfeng/roc/util/service/tracingtest/adapter/thrift"
	"github.com/shawnfeng/roc/util/service/tracingtest/pub/grpc/goodbye"
	"net/http"
)

type HelloHttp struct {}

func (p *HelloHttp) Init() error {
	fun := "HelloHttpService.Init-->"

	fmt.Printf("%s called\n", fun)
	return nil
}

func (p *HelloHttp) Driver() (string, interface{}) {
	fun := "HelloHttpService.Driver-->"
	fmt.Printf("%s called\n", fun)

	router := httprouter.New()

	router.GET("/hello", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		fun := "HelloHttpService.Hello-->"
		fmt.Printf("%s called\n", fun)
		ret := "hello"

		span := opentracing.SpanFromContext(r.Context())
		span.SetTag("by", "hello")

		//client := &http.Client{Transport: &http.Transport{}}
		//req, _ := http.NewRequest(
		//	"GET",
		//	"http://192.168.31.166:5059/hello2",
		//		nil)
		//rocserv.TraceHttpRequest(r.Context(), req)
		//resp, err := client.Do(req)
		//if err != nil {
		//	fmt.Println(err.Error())
		//} else {
		//	fmt.Println(resp)
		//}
		thrift.SayGoodbye("zhenghe", r.Context())
		grpc.SayGoodbye(r.Context(), &tracingtest.SayGoodbyeRequest{Name: "zhenghe"})

		w.Write([]byte(ret))
	})

	return ":", router
}