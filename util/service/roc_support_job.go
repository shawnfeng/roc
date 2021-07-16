package rocserv
/*
Serve() ==> server.Serve() ==> m.Init() ==> m.initServer()

initServer() 才是核心。逻辑：

newServBaseV2WithCmdArgs(): 保留，但改造。要把创建 config center 的逻辑移出来(config cneter 作为参数传入)。
* (done) initLog(): 迁出。用到了 ServConfig() 方法寻找服务配置(ServConfig 其他地方也在用，须留在 roc 中)。
* (done) stat.Init(): 迁出。这是 seaweed 中方法的简单调用。成本很低。
* initBackdoor(): 保留，专属于 roc 的功能。
* (done) initDolphin(): 迁出部分。熔断器需要迁出来，限流器保留。但熔断器可能会有问题，如果熔断器未被初始化，则会报 ErrCircuitBreakerRegistryNotInited。(且要避免熔断器被多次初始化。。。)

* (done) initfn(): 删掉。业务之初始化逻辑。需要干掉。。
* (done) initTracer(): 迁出。(初始化 xtrace，如果不初始化，像要 server_grpc.go 里面，拿到的 tracer 就是啥也不干的 tracer 了。)
* initProcessor(): 保留，属于 roc 的功能。
* initMetric(): 保留，仅属于 roc 的功能。profiling 相关。创建了一个 processor 处理 http 请求，注册了一堆 /debug/pprof/xxx 接口。


移出 roc 后，为不影响业务，需要考虑：
* 未能初始化: 业务直接升级 roc，但未重新生成代码，从而有组件未能初始化，从而导致某些功能出现问题。可能会出问题的组件有: dolphin 的熔断器未能初始化。
* 重复初始化: 重新生成了代码，但没升级 roc。不太可能出现。


initServer() 中的各种逻辑，一定要注意。
如果其不被调用，是否有别的问题？比如，因为没有被初始化，导致 panic 或者走到非预期的配置等。刚处理过的超时时间和重试次数，就是此之一例。

未决问题:
* ServBaseV2.ServConfig() 方法，当何处何从？roc 内部外部都在用这个方法，放到哪里更合适？

还有一个需要确认的点：
* 如果业务代码与 roc 中都声明了各种的 flag，会不会有冲突？比如业务代码中需要 parse 一下，而 roc 里面也需要 parse 一下。

*/

// MR 需要重新提。。
// 1. (done)修改信号 reset
// 2. (done)修改 NewServBaseV2 => NewServBaseV2WithCmdArgs。及周边
// 3. 修改上面提到的不需要的代码。

