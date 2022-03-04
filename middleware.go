package dqueue

import "context"

func newCancelMiddleware(rdb *rdb) middlewareFunc {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, m *Message) error {
			if rdb.c.Get(ctx, cancelPrefix+m.Id).Err() == nil {
				return nil
			}
			return next.Process(ctx, m)
		})
	}
}
