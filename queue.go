package dq

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// Queue defines a Queue.
type Queue struct {
	opts
	rdb

	// shutdown chan struct{}

	consumeCancel context.CancelFunc
	retryCancel   context.CancelFunc
	processCancel context.CancelFunc
	ackCancel     context.CancelFunc

	// cancelErrs chan error

	consumeDone chan error
	retryDone   chan error
	processDone chan error
	ackDone     chan error

	consumeStatus atomic.Bool
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

	if q.rdb.Client == nil {
		q.rdb.Client = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
		})
	}

	ctx := context.Background()
	if _, err := q.rdb.XGroupCreateMkStream(ctx, q.key(kReady), q.streamGroup, "0").Result(); err != nil {
		q.log(ctx, Error, "group create failed, queue: %s, err: %s", q.name, err)
	}

	go q.daemon()
	return &q
}

// Close queue
// wait for all workers exit
// 1. stop consume
// 2. stop retry
// 3. stop process
// 4. stop ack
func (q *Queue) Close(ctx context.Context) error {
	if !q.consumeStatus.Load() {
		q.log(ctx, Info, "queue %s already closed", q.name)
		return nil
	}

	defer q.consumeStatus.Store(false)

	fmt.Println("close", q.name)
	q.consumeCancel()
	select {
	case <-ctx.Done():
		q.log(ctx, Info, "queue %s close timeout", q.name)
		return ctx.Err()
	case err := <-q.consumeDone:
		if err != nil {
			q.log(ctx, Error, "consume close failed, queue: %s, err: %s", q.name, err)
		}
	}

	q.retryCancel()
	select {
	case <-ctx.Done():
		q.log(ctx, Info, "queue %s close timeout", q.name)
		return ctx.Err()
	case err := <-q.retryDone:
		if err != nil {
			q.log(ctx, Error, "retry close failed, queue: %s, err: %s", q.name, err)
		}
	}

	q.processCancel()
	select {
	case <-ctx.Done():
		q.log(ctx, Info, "queue %s close timeout", q.name)
		return ctx.Err()
	case err := <-q.processDone:
		if err != nil {
			q.log(ctx, Error, "process close failed, queue: %s, err: %s", q.name, err)
		}
	}

	q.ackCancel()
	select {
	case <-ctx.Done():
		q.log(ctx, Info, "queue %s close timeout", q.name)
		return ctx.Err()
	case err := <-q.ackDone:
		if err != nil {
			q.log(ctx, Error, "ack close failed, queue: %s, err: %s", q.name, err)
		}
	}

	q.log(ctx, Info, "queue %s closed", q.name)

	return nil
}

func (q *Queue) Destroy(ctx context.Context) error {
	q.log(ctx, Info, "destroy queue %s begin", q.name)
	_ = q.Close(ctx)
	_, err := q.rdb.Del(ctx, q.key(kReady), q.key(kDelay)).Result()
	return err
}

type redisKey int

const (
	kReady redisKey = iota
	kDelay
	kMsg
)

func (q *Queue) key(k redisKey) string {
	switch k {
	case kReady:
		return q.redisPrefix + "ready:" + q.name
	case kDelay:
		return q.redisPrefix + "delay:" + q.name
	case kMsg:
		return q.redisPrefix + "msg:" + q.name
	}
	return ""
}
