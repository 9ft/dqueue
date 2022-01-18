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
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	readyQueuePrefix = "dq:ready:"
	delayQueuePrefix = "dq:delay:"
)

// DQueue defines a queue. Must init by New()
type DQueue struct {
	name string
	rdb  *redis.Client

	daemonWorkerNum      int
	daemonWorkerInterval time.Duration

	consumeWorkerNum int
}

func New(opts *Options) *DQueue {
	var dq DQueue
	dq.init(opts)
	return &dq
}

func (d *DQueue) Close() {
}

func (d *DQueue) Destroy() {
}

func (d *DQueue) init(opts *Options) {
	d.name = opts.Name
	d.rdb = redis.NewClient(opts.RedisOpt)

	d.daemonWorkerNum = 1
	if opts.DaemonWorkerNum > 1 {
		d.daemonWorkerNum = opts.DaemonWorkerNum
	}

	d.daemonWorkerInterval = 100 * time.Millisecond
	if opts.DaemonWorkerInterval > 0 {
		d.daemonWorkerInterval = opts.DaemonWorkerInterval
	}

	d.consumeWorkerNum = 1
	if opts.ConsumeWorkerNum > 0 {
		d.consumeWorkerNum = opts.ConsumeWorkerNum
	}

	go d.daemon()
}

// Produce a message that will receive at time at. a nil time represent real-time message
func (d *DQueue) Produce(ctx context.Context, m *ProducerMessage) (id string, err error) {
	bs, err := json.Marshal(m.Value)
	if err != nil {
		return "", fmt.Errorf("json marshal failed, err: %s", err)
	}
	var v interface{}
	if err = json.Unmarshal(bs, &v); err != nil {
		return "", fmt.Errorf("json unmarshal failed, err: %s", err)
	}

	mv, err := structpb.NewValue(v)
	if err != nil {
		return "", fmt.Errorf("pb new value failed, err: %s", err)
	}
	message := &Message{
		Payload:     mv,
		Id:          uuid.NewString(),
		Retried:     0,
		ProduceTime: timestamppb.Now(),
		DeliverAt: func() *timestamppb.Timestamp {
			if m.DeliverAt.IsZero() {
				return nil
			}
			return timestamppb.New(m.DeliverAt)
		}(),
	}
	return d.produce(ctx, message)
}

func (d *DQueue) produce(ctx context.Context, m *Message) (msgID string, err error) {
	bs, _ := proto.Marshal(m)
	ms := base64.StdEncoding.EncodeToString(bs)
	err = d.enqueue(ctx, ms, m.DeliverAt.AsTime())
	if err != nil {
		return "", fmt.Errorf("enqueue failed, err: %s", err)
	}
	return m.Id, nil
}

type handler func(message *Message)

// Consume use handler to process message
func (d *DQueue) Consume(ctx context.Context, h handler) {
	go d.consume(ctx, h)
}

func (d *DQueue) consume(ctx context.Context, h handler) {
	for i := 0; i < d.consumeWorkerNum; i++ {
		go func() {
			for {
				msg := d.takeMessageBlock(ctx)
				if msg != nil {
					h(msg)
				}
			}
		}()
	}
}

func (d *DQueue) takeMessageBlock(ctx context.Context) *Message {
	msg, err := d.dequeueBlock(ctx)
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

func (d *DQueue) RedeliveryAfter(ctx context.Context, msg *Message, dur time.Duration) error {
	return d.RedeliveryAt(ctx, msg, time.Now().Add(dur))
}

func (d *DQueue) RedeliveryAt(ctx context.Context, m *Message, at time.Time) error {
	m.Retried += 1
	m.ProduceTime = timestamppb.Now()
	m.DeliverAt = timestamppb.New(at)

	_, err := d.produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce failed, err: %s", err)
	}
	return nil
}

func (d *DQueue) enqueue(ctx context.Context, msg string, at time.Time) error {
	if at.Before(time.Now()) {
		return d.rdb.RPush(ctx, readyQueuePrefix+d.name, msg).Err()
	}
	return d.rdb.ZAdd(ctx, delayQueuePrefix+d.name, &redis.Z{Score: float64(at.UnixMilli()), Member: msg}).Err()
}

func (d *DQueue) dequeueBlock(ctx context.Context) ([]string, error) {
	res, err := d.rdb.BLPop(ctx, 10*time.Second, readyQueuePrefix+d.name).Result()
	if err == redis.Nil {
		return []string{}, nil
	}
	if err != nil {
		return []string{}, fmt.Errorf("blpop failed, err: %s", err)
	}

	return res, nil
}
