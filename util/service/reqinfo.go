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
