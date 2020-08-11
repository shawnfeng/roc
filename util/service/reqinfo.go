package rocserv

import (
	"context"
	"encoding/json"

	"gitlab.pri.ibanyu.com/middleware/seaweed/xlog"
	"gitlab.pri.ibanyu.com/middleware/util/idl/gen-go/util/thriftutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const ContextKeyHead = "Head"

func reqInfoInjectServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			xlog.Debugf(ctx, "no metadata from context")
			return handler(ctx, req)
		}

		values := md.Get(ContextKeyHead)
		if len(values) == 0 {
			xlog.Debugf(ctx, "no head in metadata")
			return handler(ctx, req)
		}

		var head thriftutil.Head
		if err := json.Unmarshal([]byte(values[0]), &head); err != nil {
			xlog.Warnf(ctx, "unmarshal head error, string: %s, err: %v", values[0], err)
			return handler(ctx, req)
		}

		ctx = context.WithValue(ctx, ContextKeyHead, head)
		return handler(ctx, req)
	}
}

func reqInfoInjectClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, resp interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		head := ctx.Value(ContextKeyHead)
		if head == nil {
			xlog.Debugf(ctx, "context Head is nil")
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		ohead, ok := head.(*thriftutil.Head)
		if !ok {
			xlog.Warnf(ctx, "invalid head type: %T", head)
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		bytes, err := json.Marshal(ohead)
		if err != nil {
			xlog.Warnf(ctx, "marshal head error, head: %+v, err: %v", head, err)
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		ctx = metadata.AppendToOutgoingContext(ctx, ContextKeyHead, string(bytes))
		return invoker(ctx, method, req, resp, cc, opts...)
	}
}
