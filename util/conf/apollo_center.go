package conf

import (
	"context"
	"sync"

	"github.com/shawnfeng/sutil/sconf/center"
)

// ApolloCenter ...
type ApolloCenter struct {
	watchOnce sync.Once
	ch        chan *center.ChangeEvent
	center    center.ConfigCenter
}

// NewApolloCenter ...
func NewApolloCenter() *ApolloCenter {
	return &ApolloCenter{
		ch: make(chan *center.ChangeEvent),
	}
}

// Init ...
func (m *ApolloCenter) Init(ctx context.Context, appID string, namespaces []string) error {
	apolloCenter, err := center.NewConfigCenter(center.ApolloConfigCenter)
	if err != nil {
		return err
	}

	err = apolloCenter.Init(ctx, appID, namespaces)
	if err != nil {
		return err
	}

	m.center = apolloCenter
	return err
}

type apolloObserver struct {
	ch chan<- *center.ChangeEvent
}

func (ob *apolloObserver) HandleChangeEvent(event *center.ChangeEvent) {
	var changes = map[string]*center.Change{}
	for k, ce := range event.Changes {
		changes[k] = ce
	}
	event.Changes = changes
	ob.ch <- event
}

// Watch ...
func (m *ApolloCenter) Watch(ctx context.Context) <-chan *center.ChangeEvent {
	m.watchOnce.Do(func() {
		m.center.StartWatchUpdate(ctx)
		m.center.RegisterObserver(ctx, &apolloObserver{m.ch})
	})

	return m.ch
}

// GetCenter ...
func (m *ApolloCenter) GetCenter() center.ConfigCenter {
	return m.center
}
