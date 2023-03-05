package dq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Queue defines a Queue.
type Queue struct {
	opts
	rdb

	shutdown chan struct{}
	done     chan struct{}
}

type rdb struct {
	*redis.Client
	redisPrefix string
}

func New(options ...func(*Queue)) *Queue {
	q := Queue{
		opts: defaultOpts(),
		rdb:  rdb{redisPrefix: "dq:"},
	}

	for _, opt := range options {
		opt(&q)
	}

	q.shutdown = make(chan struct{})
	q.done = make(chan struct{})

	if q.rdb.Client == nil {
		q.rdb.Client = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
		})
	}

	go q.daemon()
	return &q
}

func (q *Queue) Close(ctx context.Context) error {
	close(q.shutdown)

	select {
	case <-q.done:
		q.log(ctx, Info, "queue %s closed", q.name)
	case <-ctx.Done():
		q.log(ctx, Error, "queue %s closed with err: %v", q.name, ctx.Err())
		return ctx.Err()
	}

	return nil
}

type redisKey int

const (
	kReady redisKey = iota
	kDelay
	kRetry
	kMsg
)

func (q *Queue) getKey(k redisKey) string {
	switch k {
	case kReady:
		return q.redisPrefix + "ready:" + q.name
	case kDelay:
		return q.redisPrefix + "delay:" + q.name
	case kRetry:
		return q.redisPrefix + "retry:" + q.name
	case kMsg:
		return q.redisPrefix + "msg:" + q.name
	}
	return ""
}
