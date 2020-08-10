package rocserv

import (
	"context"
	"encoding/json"
	"fmt"

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

		head, err := extractHeadFromMD(md)
		if err != nil {
			xlog.Warnf(ctx, "extractHeadFromMD error: %v", err)
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
			xlog.Warnf(ctx, "context Head is nil")
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		ohead, ok := head.(*thriftutil.Head)
		if !ok {
			xlog.Warnf(ctx, "invalid head type: %T", head)
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		bytes, err := json.Marshal(ohead)
		if err != nil {
			xlog.Warnf(ctx,"marshal head error, head: %+v, err: %v", head, err)
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		ctx = metadata.AppendToOutgoingContext(ctx, ContextKeyHead, string(bytes))
		return invoker(ctx, method, req, resp, cc, opts...)
	}
}

func extractHeadFromMD(md metadata.MD) (*thriftutil.Head, error) {
	if md == nil {
		return nil, fmt.Errorf("nil metadata")
	}
	values := md.Get(ContextKeyHead)
	if len(values) == 0 {
		return nil, fmt.Errorf("no head in metadata")
	}
	var head thriftutil.Head
	if err := json.Unmarshal([]byte(values[0]), &head); err != nil {
		return nil, fmt.Errorf("unmarshal head error, string: %s, err: %v", values[0], err)
	}
	return &head, nil
}

func addHeadIntoMD(md metadata.MD, head *thriftutil.Head) error {
	if md == nil {
		return fmt.Errorf("md is nil")
	}
	if head == nil {
		return fmt.Errorf("head is nil")
	}
	bytes, err := json.Marshal(head)
	if err != nil {
		return fmt.Errorf("marshal head error, head: %+v, err: %v", head, err)
	}
	md.Append(ContextKeyHead, string(bytes))
	return nil
}
