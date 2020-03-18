package conf

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApolloConfig_Init(t *testing.T) {
	ass := assert.New(t)
	cfg := NewApolloCenter()
	err := cfg.Init(context.TODO(), "test", []string{"application"})
	ass.Nil(err)
}

func TestApolloConfig_Watch(t *testing.T) {
	ass := assert.New(t)
	cfg := NewApolloCenter()
	cfg.Init(context.TODO(), "test", []string{"application"})
	ceChan := cfg.Watch(context.TODO())
	ass.NotNil(ceChan)
}
