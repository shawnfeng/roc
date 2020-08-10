package rocserv

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.pri.ibanyu.com/middleware/util/idl/gen-go/util/thriftutil"
	"google.golang.org/grpc/metadata"
)

func Test_extractHeadFromMD(t *testing.T) {
	// prepare
	head := createTestHead()
	bytes, err := json.Marshal(head)
	assert.NoError(t, err)
	md := metadata.MD{}
	md.Set(ContextKeyHead, string(bytes))

	// execute
	retHead, err := extractHeadFromMD(md)

	// verify
	assert.NoError(t, err)
	assert.Equal(t, head, retHead)
}

func createTestHead() *thriftutil.Head {
	head := thriftutil.Head{
		Uid:     123,
		Source:  456,
		Ip:      "127.0.0.1",
		Region:  "CN",
		Dt:      11,
		Unionid: "abc",
	}
	return &head
}
