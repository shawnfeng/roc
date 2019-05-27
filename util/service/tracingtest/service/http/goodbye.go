package http

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/opentracing/opentracing-go"
	"net/http"
)

type GoodbyeHttp struct{}

func (p *GoodbyeHttp) Init() error {
	fun := "GoodbyeHttpService.Init-->"

	fmt.Printf("%s called\n", fun)
	return nil
}

func (p *GoodbyeHttp) Driver() (string, interface{}) {
	fun := "GoodbyeHtppService.Driver-->"
	fmt.Printf("%s called\n", fun)

	router := httprouter.New()

	router.GET("/say-good-bye", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		fun := "HelloHttpService.SayGoodbye-->"
		fmt.Printf("%s called\n", fun)
		ret := "goodbye"

		span := opentracing.SpanFromContext(r.Context())
		span.SetTag("by", "http-goodbye")

		w.Write([]byte(ret))
	})

	return ":", router
}
