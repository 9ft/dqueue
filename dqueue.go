// Package dqueue defines message queues, supporting real-time and delayed message
// Use redis list as ready queue. Use redis sorted set as delay queue.
package dqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Queue defines a queue.
type Queue struct {
	// basic
	name string
	rdb  *rdb

	// daemon
	daemonWorkerNum      int
	daemonWorkerInterval time.Duration

	// consumer
	consumeWorkerNum int
	maxRetryTimes    int

	mws          []middlewareFunc
	enableCancel bool
}

func New(opts *Options) *Queue {
	var q Queue
	q.opts(opts)
	go q.daemon()
	return &q
}

func (q *Queue) Close() {
	// TODO
}

func (q *Queue) Destroy() {
	// TODO
}

func (q *Queue) opts(opts *Options) {
	q.name = opts.Name
	q.rdb = &rdb{redis.NewClient(opts.RedisOpt)}

	q.daemonWorkerNum = 1
	if opts.DaemonWorkerNum > 1 {
		q.daemonWorkerNum = opts.DaemonWorkerNum
	}

	q.daemonWorkerInterval = 100 * time.Millisecond
	if opts.DaemonWorkerInterval >= 0 {
		q.daemonWorkerInterval = opts.DaemonWorkerInterval
	}

	q.consumeWorkerNum = 1
	if opts.ConsumeWorkerNum > 0 {
		q.consumeWorkerNum = opts.ConsumeWorkerNum
	}

	q.enableCancel = opts.EnableCancel
}

const (
	readyPrefix  = "dq:ready:"
	delayPrefix  = "dq:delay:"
	cancelPrefix = "dq:cancel:"
)

type rdb struct {
	c *redis.Client
}

type HandlerFunc func(context.Context, *Message) error

func (h HandlerFunc) Process(ctx context.Context, message *Message) error {
	return h(ctx, message)
}

type Handler interface {
	Process(context.Context, *Message) error
}

type middlewareFunc func(Handler) Handler

func (q *Queue) Use(mws ...middlewareFunc) {
	for _, f := range mws {
		q.mws = append(q.mws, f)
	}
}

// Produce a message that will receive at time at. a nil time represent real-time message
func (q *Queue) Produce(ctx context.Context, m *ProducerMessage) (id string, err error) {
	payload := m.Payload
	if m.Value != nil {
		if payload, err = json.Marshal(m.Value); err != nil {
			return "", fmt.Errorf("json marshal failed, err: %s", err)
		}
	}

	message := &Message{
		Payload:     payload,
		Id:          uuid.NewString(),
		Retried:     0,
		ProduceTime: timestamppb.Now(),
		DeliverAt: func() *timestamppb.Timestamp {
			if m.DeliverAt == nil || m.DeliverAt.IsZero() {
				return nil
			}
			return timestamppb.New(*m.DeliverAt)
		}(),
	}

	return q.produce(ctx, message)
}

func (q *Queue) produce(ctx context.Context, m *Message) (msgID string, err error) {
	bs, _ := proto.Marshal(m)
	ms := base64.StdEncoding.EncodeToString(bs)

	err = q.enqueue(ctx, ms, m.DeliverAt.AsTime())
	if err != nil {
		return "", fmt.Errorf("enqueue failed, err: %s", err)
	}
	return m.Id, nil
}

// Consume use handler to process message
func (q *Queue) Consume(ctx context.Context, h Handler) {
	go q.consume(ctx, h)
}

func (q *Queue) consume(ctx context.Context, h Handler) {
	if q.enableCancel {
		q.mws = append([]middlewareFunc{newCancelMiddleware(q.rdb)}, q.mws...)
	}

	for i := len(q.mws) - 1; i >= 0; i-- {
		h = q.mws[i](h)
	}

	for i := 0; i < q.consumeWorkerNum; i++ {
		go func() {
			for {
				msg := q.takeMessage(ctx)
				if msg != nil {
					_ = h.Process(ctx, msg)
				}
			}
		}()
	}
}

func (m *Message) GetSchemaValue(v interface{}) error {
	return json.Unmarshal(m.Payload, v)
}

func (q *Queue) takeMessage(ctx context.Context) *Message {
	msg, err := q.dequeueBlock(ctx)
	if err != nil || len(msg) < 2 {
		return nil
	}
	bs, _ := base64.StdEncoding.DecodeString(msg[1])
	var m Message
	err = proto.Unmarshal(bs, &m)
	if err != nil {
		return nil
	}
	return &m
}

func (q *Queue) RedeliveryAfter(ctx context.Context, msg *Message, dur time.Duration) error {
	return q.RedeliveryAt(ctx, msg, time.Now().Add(dur))
}

func (q *Queue) RedeliveryAt(ctx context.Context, m *Message, at time.Time) error {
	m.Retried += 1
	m.ProduceTime = timestamppb.Now()
	m.DeliverAt = timestamppb.New(at)

	_, err := q.produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce failed, err: %s", err)
	}
	return nil
}

func (q *Queue) enqueue(ctx context.Context, msg string, at time.Time) error {
	if at.Before(time.Now()) {
		return q.rdb.c.RPush(ctx, readyPrefix+q.name, msg).Err()
	}
	return q.rdb.c.ZAdd(ctx, delayPrefix+q.name, &redis.Z{Score: float64(at.UnixMilli()), Member: msg}).Err()
}

func (q *Queue) dequeueBlock(ctx context.Context) ([]string, error) {
	res, err := q.rdb.c.BLPop(ctx, 10*time.Second, readyPrefix+q.name).Result()
	if err == redis.Nil {
		return []string{}, nil
	}
	if err != nil {
		return []string{}, fmt.Errorf("blpop failed, err: %s", err)
	}

	return res, nil
}

func (q *Queue) Cancel(ctx context.Context, id string) error {
	if !q.enableCancel {
		return fmt.Errorf("cancel not enabled")
	}
	return q.rdb.c.SetEX(ctx, cancelPrefix+id, 1, 1*time.Hour).Err()
}
