package rocserv

import (
	"github.com/julienschmidt/httprouter"
)

type Middleware func(httprouter.Handle) httprouter.Handle

type MiddlewareQueue struct {
	middlewares []Middleware
}

func NewMiddlewareQueue() *MiddlewareQueue {
	return &MiddlewareQueue{
		middlewares: []Middleware{},
	}
}

func (s *MiddlewareQueue) Use(mw ...Middleware) {
	s.middlewares = append(s.middlewares, mw...)
}

func (s *MiddlewareQueue) Wrap(fn httprouter.Handle) httprouter.Handle {
	if len(s.middlewares) == 0 {
		return fn
	}

	// There is at least one item in the middleware list.
	result := s.middlewares[0](fn)

	for _, m := range s.middlewares {
		result = m(result)
	}

	return result
}
