package rocserv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gitlab.pri.ibanyu.com/middleware/dolphin/rate_limit"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xcontext"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xerror"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	xprom "gitlab.pri.ibanyu.com/middleware/seaweed/xstat/xmetric/xprometheus"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtime"
	"gitlab.pri.ibanyu.com/middleware/seaweed/xtrace"
	"gitlab.pri.ibanyu.com/middleware/util/idl/gen-go/util/thriftutil"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/julienschmidt/httprouter"
	"github.com/uber/jaeger-client-go"
	"google.golang.org/grpc/codes"
)

const (
	HttpHeaderKeyTraceID = "ipalfish-trace-id"

	WildCharacter = ":"

	RoutePath = "req-simple-path"

	HttpHeaderKeyGroup = "ipalfish-group"
	HttpHeaderKeyHead  = "ipalfish-head"

	CookieNameGroup = "ipalfish_group"

	ReqHeaderKey = "req_header"
)

// HttpServer is the http server, Create an instance of GinServer, by using NewGinServer()
type HttpServer struct {
	*gin.Engine
}

// Context warp gin Context
type Context struct {
	*gin.Context
}

// ReqHeader 请求规范中，在请求参数中统一包装的一些内容，参考：http://midplatform.book.pri.ibanyu.com/doc/standard/interface.html
type ReqHeader struct {
	Token string `json:"token"`
	Uid   int64  `json:"h_m"`

	Did     string `json:"h_did"`
	Ver     string `json:"h_av"`
	Model   string `json:"h_model"` // 设备型号
	Dt      int32  `json:"h_dt"`
	Dtsub   int32  `json:"h_dt_sub"`
	Ch      string `json:"h_ch"`
	Net     int32  `json:"h_nt"`
	Unionid string `json:"h_unionid"`
	Het     int32  `json:"h_et"` // 终端类型，0其他；1APP；2web或H5；3微信；4小程序

	HLc  string `json:"h_lc"`
	Cate int32  `json:"cate"`

	Source    int32  `json:"h_src"`
	Zone      int32  `json:"zone"`
	Subsource int32  `json:"h_sub_src"`
	ZoneName  string `json:"zone_name"`
}

func NewGinServer() *gin.Engine {
	fun := "NewGinServer -->"

	router := gin.New()

	middlewares := []gin.HandlerFunc{Recovery(), AccessLog(), Trace(), RateLimit(), Metric(), InjectFromRequest()}
	disableContextCancel := isDisableContextCancel()
	if disableContextCancel {
		middlewares = append(middlewares, DisableContextCancel())
	}
	xlog.Infof(context.TODO(), "%s disableContextCancel: %v", fun, disableContextCancel)

	router.Use(middlewares...)

	// 404 处理
	router.NoRoute(NotFound())

	return router
}

// HandlerFunc ...
type HandlerFunc func(*Context)

// NewHttpServer create http server with gin
func NewHttpServer() *HttpServer {
	router := NewGinServer()

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

func isDisableContextCancel() bool {
	if r, ok := GetConfigCenter().GetBool(context.Background(), disableContextCancelKey); ok {
		return r
	}
	return false
}

func DisableContextCancel() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(xcontext.NewValueContext(c.Request.Context()))
		c.Next()
	}
}

func InjectFromRequest() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		if _, ok := c.Get(ReqHeaderKey); !ok {
			ctx = extractContextHeadFromReqHeader(ctx, c.Request)
		}
		ctx = extractContextControlFromRequest(ctx, c.Request)
		ctx = extractContextHeadFromRequest(ctx, c.Request)
		c.Request = c.Request.WithContext(ctx)
	}
}

// Metric returns a metric middleware
func Metric() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		ctx = contextWithErrCode(ctx, 1)
		c.Request = c.Request.WithContext(ctx)

		now := time.Now()
		c.Next()
		dt := time.Since(now)

		errCode := getErrCodeFromContext(c.Request.Context())

		path := c.FullPath()
		path = ParseUriApi(path)

		group, serviceName := GetGroupAndService()
		_metricAPIRequestCount.With(xprom.LabelGroupName, group, xprom.LabelServiceName, serviceName, xprom.LabelAPI, path, xprom.LabelErrCode, strconv.Itoa(errCode)).Inc()
		_metricAPIRequestTime.With(xprom.LabelGroupName, group, xprom.LabelServiceName, serviceName, xprom.LabelAPI, path, xprom.LabelErrCode, strconv.Itoa(errCode)).Observe(float64(dt / time.Millisecond))
	}
}

// Trace returns a trace middleware
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {
		span := xtrace.SpanFromContext(c.Request.Context())
		if span == nil {
			newSpan, ctx := xtrace.StartSpanFromContext(c.Request.Context(), c.FullPath())
			c.Request = c.Request.WithContext(ctx)
			span = newSpan
		}
		defer span.Finish()

		if sc, ok := span.Context().(jaeger.SpanContext); ok {
			c.Writer.Header()[HttpHeaderKeyTraceID] = []string{fmt.Sprint(sc.TraceID())}
		}

		c.Next()
	}
}

func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		st := xtime.NewTimeStat()
		path := c.Request.URL.Path
		bodyData, _ := c.GetRawData()
		c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(bodyData))
		blw := &bodyLogWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = blw

		c.Next()

		method := c.Request.Method
		statusCode := c.Writer.Status()
		ctx := c.Request.Context()

		keyAndValue := []interface{}{
			"path", path,
			"cost", st.Millisecond(),
			"method", method,
			"code", statusCode,
		}
		if shouldHttpLogRequest(path) {
			keyAndValue = append(keyAndValue, "req", string(bodyData), "resp", blw.body.String())
		}
		xlog.Infow(ctx, "", keyAndValue...)
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

func RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		path := c.FullPath()
		// 为了使dolphin存储结构更加清晰，采用的替换
		path = strings.ReplaceAll(path, "/", "$")
		caller := GetCallerFromBaggage(ctx)
		err := rateLimitRegistry.InterfaceRateLimit(ctx, path, caller)
		if err != nil {
			code := codes.Internal
			if err == rate_limit.ErrRateLimited {
				xlog.Warnf(ctx, "rate limited: path=%s, caller=%s", c.FullPath(), caller)
				code = xerror.RateLimited
			}
			httpStatus := xerror.MapErrorCodeToHTTPStatusCode(code)
			errorResponse := &ErrorResponseBody{
				Ret:  -1,
				Code: int32(code),
				Msg:  http.StatusText(httpStatus),
			}
			c.AbortWithStatusJSON(httpStatus, errorResponse)
			return
		}
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

func extractContextControlFromRequest(ctx context.Context, req *http.Request) context.Context {
	var group string
	if group = extractRouteGroupFromHost(req); group != "" {
		return injectRouteGroupToContext(ctx, group)
	}

	if group = extractRouteGroupFromHeader(req); group != "" {
		return injectRouteGroupToContext(ctx, group)
	}

	if group = extractRouteGroupFromCookie(req); group != "" {
		return injectRouteGroupToContext(ctx, group)
	}

	return injectRouteGroupToContext(ctx, xcontext.DefaultGroup)
}

func extractContextHeadFromRequest(ctx context.Context, req *http.Request) context.Context {
	// NOTE: 如果已经有了就先不覆盖
	val := ctx.Value(xcontext.ContextKeyHead)
	if val != nil {
		return ctx
	}

	headJsonString := req.Header.Get(HttpHeaderKeyHead)
	var head thriftutil.Head
	_ = json.Unmarshal([]byte(headJsonString), &head)
	ctx = context.WithValue(ctx, xcontext.ContextKeyHead, &head)
	return ctx
}

var domainRouteRegexp = regexp.MustCompile(`(?P<group>.+)\.group\..+`)

func extractRouteGroupFromHost(r *http.Request) (group string) {
	matches := domainRouteRegexp.FindStringSubmatch(r.Host)
	names := domainRouteRegexp.SubexpNames()
	for i, _ := range matches {
		if names[i] == "group" {
			group = matches[i]
		}
	}
	return
}

func injectRouteGroupToContext(ctx context.Context, group string) context.Context {
	control := thriftutil.NewDefaultControl()
	control.Route.Group = group

	return context.WithValue(ctx, xcontext.ContextKeyControl, control)
}

func extractRouteGroupFromHeader(r *http.Request) (group string) {
	return r.Header.Get(HttpHeaderKeyGroup)
}

func extractRouteGroupFromCookie(r *http.Request) (group string) {
	ck, err := r.Cookie(CookieNameGroup)
	if err == nil {
		group = ck.Value
	}
	return
}

type ErrCode struct {
	int
}

const ErrCodeKey = "ErrCode"

func ContextSetErrCode(ctx context.Context, errCode int) {
	errCodeContext, ok := ctx.Value(ErrCodeKey).(*ErrCode)
	if !ok {
		return
	}
	errCodeContext.int = errCode
	if span := xtrace.SpanFromContext(ctx); span != nil {
		span.SetTag("errcode", errCode)
	}
}

func contextWithErrCode(ctx context.Context, errCode int) context.Context {
	if span := xtrace.SpanFromContext(ctx); span != nil {
		span.SetTag("errcode", errCode)
	}
	return context.WithValue(ctx, ErrCodeKey, &ErrCode{errCode})
}

func getErrCodeFromContext(ctx context.Context) int {
	errCode, ok := ctx.Value(ErrCodeKey).(*ErrCode)
	if !ok {
		return 1
	}
	return errCode.int
}

func shouldHttpLogRequest(path string) bool {
	// 默认打印body
	center := GetConfigCenter()
	if center == nil {
		return true
	}
	printBodyMethod := printBodyMethod{}

	// 方法配置
	_ = center.Unmarshal(context.Background(), &printBodyMethod)
	// 不打印的优先级更高
	if methodInList(path, printBodyMethod.NoLogRequestMethodList) {
		return false
	}
	if methodInList(path, printBodyMethod.LogRequestMethodList) {
		return true
	}

	// 全局配置
	isPrint, ok := center.GetBool(context.Background(), logRequestKey)
	if !ok {
		// 默认输出
		return true
	}
	return isPrint
}

// gin打印response的方式
type bodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w bodyLogWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

// ErrorResponseBody HTTP 层出错时的标准响应。
type ErrorResponseBody struct {
	Ret  int32  `json:"ret"`
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}

// NotFound NotFound returns a 404 not found handle
func NotFound() gin.HandlerFunc {
	return func(c *gin.Context) {
		nfResp := &ErrorResponseBody{
			Ret:  -1,
			Code: int32(codes.NotFound),
			Msg:  http.StatusText(http.StatusNotFound),
		}

		c.JSON(http.StatusNotFound, nfResp)
		return
	}
}

func extractContextHeadFromReqHeader(ctx context.Context, req *http.Request) context.Context {
	reqHeader := new(ReqHeader)
	bodyData, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return ctx
	}
	// set body
	req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyData))
	err = json.Unmarshal(bodyData, reqHeader)
	if err != nil {
		return ctx
	}
	head := &thriftutil.Head{}
	val := ctx.Value(xcontext.ContextKeyHead)
	if val != nil {
		if vh, ok := val.(*thriftutil.Head); ok {
			head = vh
		}
	}
	// 暂时先加一个hlc字段
	if head.Properties == nil {
		head.Properties = make(map[string]string)
	}
	head.Properties[xcontext.ContextPropertiesKeyHLC] = reqHeader.HLc
	ctx = context.WithValue(ctx, xcontext.ContextKeyHead, head)
	return ctx
}
