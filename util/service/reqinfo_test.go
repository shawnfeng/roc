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

func Test_addHeadIntoMD(t *testing.T) {
	md := metadata.MD{}
	head := createTestHead()
	err := addHeadIntoMD(md, head)

	assert.NoError(t, err)

	bytes, err := json.Marshal(head)
	assert.NoError(t, err)
	ret := md.Get(ContextKeyHead)
	assert.Equal(t, 1, len(ret))
	assert.Equal(t, string(bytes), ret[0])
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
