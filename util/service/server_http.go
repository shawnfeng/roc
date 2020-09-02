package rocserv

import (
	"fmt"
	"net/http/httputil"
	"runtime"
	"strings"
	"time"

	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/julienschmidt/httprouter"
	"github.com/uber/jaeger-client-go"
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

// WGET is wrap httprouter handle to GET
func (s *HttpServer) WGET(relativePath string, handlers ...httprouter.Handle) {
	s.GET(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// POST is a shortcut for router.Handle("POST", path, handle).
func (s *HttpServer) POST(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.POST(relativePath, ws...)
}

// WPOST is wrap httprouter handle to POST
func (s *HttpServer) WPOST(relativePath string, handlers ...httprouter.Handle) {
	s.POST(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// PUT is a shortcut for router.Handle("PUT", path, handle).
func (s *HttpServer) PUT(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.PUT(relativePath, ws...)
}

// WPUT is wrap httprouter handle to PUT
func (s *HttpServer) WPUT(relativePath string, handlers ...httprouter.Handle) {
	s.PUT(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// Any registers a route that matches all the HTTP methods.
func (s *HttpServer) Any(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.Any(relativePath, ws...)
}

// WAny is wrap httprouter handle to ANY
func (s *HttpServer) WAny(relativePath string, handlers ...httprouter.Handle) {
	s.Any(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// DELETE is a shortcut for router.Handle("DELETE", path, handle).
func (s *HttpServer) DELETE(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.DELETE(relativePath, ws...)
}

// WDELETE is wrap httprouter handle to DELETE
func (s *HttpServer) WDELETE(relativePath string, handlers ...httprouter.Handle) {
	s.DELETE(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// PATCH is a shortcut for router.Handle("PATCH", path, handle).
func (s *HttpServer) PATCH(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.PATCH(relativePath, ws...)
}

// WPATCH is wrap httprouter handle to PATCH
func (s *HttpServer) WPATCH(relativePath string, handlers ...httprouter.Handle) {
	s.PATCH(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// OPTIONS is a shortcut for router.Handle("OPTIONS", path, handle).
func (s *HttpServer) OPTIONS(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.OPTIONS(relativePath, ws...)
}

// WOPTIONS is wrap httprouter handle to OPTIONS
func (s *HttpServer) WOPTIONS(relativePath string, handlers ...httprouter.Handle) {
	s.OPTIONS(relativePath, MutilWrapHttpRouter(handlers...)...)
}

// HEAD is a shortcut for router.Handle("HEAD", path, handle).
func (s *HttpServer) HEAD(relativePath string, handlers ...HandlerFunc) {
	ws := append([]gin.HandlerFunc{pathHook(relativePath)}, mutilWrap(handlers...)...)
	s.Engine.HEAD(relativePath, ws...)
}

// WHEAD is wrap httprouter handle to HEAD
func (s *HttpServer) WHEAD(relativePath string, handlers ...httprouter.Handle) {
	s.HEAD(relativePath, MutilWrapHttpRouter(handlers...)...)
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
		span := xtrace.SpanFromContext(c.Request.Context())
		if span == nil {
			newSpan, ctx := xtrace.StartSpanFromContext(c.Request.Context(), c.Request.RequestURI)
			c.Request.WithContext(ctx)
			span = newSpan
		}
		defer span.Finish()

		if sc, ok := span.Context().(jaeger.SpanContext); ok {
			c.Writer.Header()[HttpHeaderKeyTraceID] = []string{fmt.Sprint(sc.TraceID())}
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

// MutilWrapHttpRouter wrap many httprouter handle to roc httpserver handle
func MutilWrapHttpRouter(handlers ...httprouter.Handle) []HandlerFunc {
	var h = make([]HandlerFunc, len(handlers))
	for k, v := range handlers {
		h[k] = WrapHttpRouter(v)
	}
	return h
}

// WrapHttpRouter wrap httprouter handle to roc httpserver handle
func WrapHttpRouter(handle httprouter.Handle) HandlerFunc {
	return func(c *Context) {
		params := make([]httprouter.Param, 0)
		for param := range c.Params {
			params = append(params, httprouter.Param{
				Key:   c.Params[param].Key,
				Value: c.Params[param].Value,
			})
		}
		handle(c.Writer, c.Request, params)
	}
}
