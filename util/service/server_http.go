package rocserv

import (
	"fmt"
	"net/http/httputil"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin/binding"

	"github.com/uber/jaeger-client-go"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"

	"github.com/gin-gonic/gin"
)

const (
	HttpHeaderKeyTraceID = "ipalfish-trace-id"

	WildCharacter = ":"
	RoutePath     = "req-simple-path"
)

// HttpServer is the http server, Create an instance of GinServer, by using NewGinServer()
type HttpServer struct {
	*gin.Engine
}

// Context warp gin Context
type Context struct {
	*gin.Context
}

// HandlerFunc ...
type HandlerFunc func(*Context)

// NewHttpServer create http server with gin
func NewHttpServer() *HttpServer {
	// 实例化gin Server
	router := gin.New()
	router.Use(Recovery(), Metric(), Trace())

	return &HttpServer{router}
}

// Use attachs a global middleware to the router
func (s *HttpServer) Use(middleware ...HandlerFunc) {
	s.Engine.Use(mutilWrap(middleware...)...)
}

// GET is a shortcut for router.Handle("GET", path, handle).
func (s *HttpServer) GET(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.GET(relativePath, ws...)
}

// POST is a shortcut for router.Handle("POST", path, handle).
func (s *HttpServer) POST(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.POST(relativePath, ws...)
}

// PUT is a shortcut for router.Handle("PUT", path, handle).
func (s *HttpServer) PUT(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.PUT(relativePath, ws...)
}

// Any registers a route that matches all the HTTP methods.
func (s *HttpServer) Any(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.Any(relativePath, ws...)
}

// DELETE is a shortcut for router.Handle("DELETE", path, handle).
func (s *HttpServer) DELETE(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.DELETE(relativePath, ws...)
}

// PATCH is a shortcut for router.Handle("PATCH", path, handle).
func (s *HttpServer) PATCH(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.PATCH(relativePath, ws...)
}

// OPTIONS is a shortcut for router.Handle("OPTIONS", path, handle).
func (s *HttpServer) OPTIONS(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.OPTIONS(relativePath, ws...)
}

// HEAD is a shortcut for router.Handle("HEAD", path, handle).
func (s *HttpServer) HEAD(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.HEAD(relativePath, ws...)
}

// Bind checks the Content-Type to select a binding engine automatically
func (c *Context) Bind(obj interface{}) error {
	b := binding.Default(c.Request.Method, c.ContentType())
	return c.MustBindWith(obj, b)
}

// Metric returns a metric middleware
func Metric() gin.HandlerFunc {
	return func(c *gin.Context) {
		now := time.Now()
		c.Next()
		dt := time.Since(now)
		if path, exist := c.Get(RoutePath); exist {
			if fun, ok := path.(string); ok {
				group, serviceName := GetGroupAndService()
				_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, serviceName, xprom.LabelAPI, fun).Inc()
				_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, serviceName, xprom.LabelAPI, fun).Observe(float64(dt / time.Millisecond))
			}
		}
	}
}

// Trace returns a trace middleware
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {
		if span := xtrace.SpanFromContext(c.Request.Context()); span != nil {
			if sc, ok := span.Context().(jaeger.SpanContext); ok {
				c.Writer.Header()[HttpHeaderKeyTraceID] = []string{fmt.Sprint(sc.TraceID())}
			}
		}
		c.Next()
	}
}

// Recovery returns a middleware that recovers from any panics and writes a 500 if there was one.
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			var rawReq []byte
			if err := recover(); err != nil {
				const size = 64 << 10
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				if c.Request != nil {
					rawReq, _ = httputil.DumpRequest(c.Request, false)
				}
				pl := fmt.Sprintf("http call panic: %s\n%v\n%s\n", string(rawReq), err, buf)
				fmt.Println(pl)
				c.AbortWithStatus(500)
			}
		}()
		c.Next()
	}
}

func pathHook(relativePath string) gin.HandlerFunc {
	return func(c *gin.Context) {
		values := strings.Split(relativePath, WildCharacter)
		c.Set(RoutePath, values[0])
	}
}

func mutilWrap(handlers ...HandlerFunc) []gin.HandlerFunc {
	var h = make([]gin.HandlerFunc, len(handlers))
	for k, v := range handlers {
		h[k] = wrap(v)
	}
	return h
}

func wrap(h HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		h(&Context{c})
	}
}
