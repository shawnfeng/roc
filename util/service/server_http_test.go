package rocserv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

type HelloReq struct {
	Content string `json:"content"`
}

type UserReq struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty" form:"name" validate:"required"`
}

func init() {
	go startServer("127.0.0.1:19999")
}

func startServer(addr string) {
	s := NewHttpServer()
	s.POST("/hello", func(ctx *Context) {
		p := &HelloReq{}
		if err := ctx.Bind(p); err != nil {
			return
		}
		ctx.JSON(200, p)
	})
	s.POST("/user/:id", func(ctx *Context) {
		p := new(UserReq)
		if err := ctx.Bind(p); err != nil {
			return
		}
		ctx.JSON(200, nil)
	})
	server := &http.Server{
		Addr:    addr,
		Handler: s,
	}
	_ = server.ListenAndServe()
}

func TestHello(t *testing.T) {
	var jsonStr = []byte(`{"content":"hello."}`)
	req, err := http.NewRequest("POST", "http://127.0.0.1:19999/hello", bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	ass := assert.New(t)
	ass.Nil(err)
	body, err := ioutil.ReadAll(resp.Body)
	ass.Nil(err)
	ass.NotNil(body)
	fmt.Println(string(body))
}

func TestUser(t *testing.T) {
	var jsonStr = []byte(`{"name":"test"}`)
	req, err := http.NewRequest("POST", "http://127.0.0.1:19999/user/1", bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	ass := assert.New(t)
	ass.Nil(err)
	body, err := ioutil.ReadAll(resp.Body)
	ass.Nil(err)
	ass.NotNil(body)
	fmt.Println(string(body))
}

func TestRouterNotFound(t *testing.T) {
	ass := assert.New(t)
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:19999/helloworld", nil)
	ass.Nil(err)

	client := &http.Client{}
	resp, err := client.Do(req)
	ass.Nil(err)
	ass.Equal(404, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	ass.Nil(err)
	ass.NotNil(body)

	nfResp := &ErrorResponseBody{
		Ret:  -1,
		Code: int32(codes.NotFound),
		Msg:  http.StatusText(http.StatusNotFound),
	}

	actualResp := new(ErrorResponseBody)
	err = json.Unmarshal(body, actualResp)
	ass.Nil(err)
	ass.Equal(nfResp, actualResp)
}
