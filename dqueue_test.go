package dqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var testRdb = &redis.Options{Addr: "127.0.0.1:6379"}

var PrintMiddleWare = func(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, t *Message) error {
		log.Println("Print:", t)
		return next.Process(ctx, t)
	})
}

func TestProduceAndConsume(t *testing.T) {
	ctx := context.Background()

	dq := New(&Options{
		Name:                 "test",
		RedisOpt:             testRdb,
		DaemonWorkerInterval: 0,
	})

	size := 1000
	var wg sync.WaitGroup
	wg.Add(size)

	dq.Consume(ctx, HandlerFunc(func(ctx context.Context, m *Message) error {
		wg.Done()
		return nil
	}))
	go func() {
		for i := 0; i < size; i++ {
			go dq.Produce(ctx, &ProducerMessage{Value: i})
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-time.Tick(3 * time.Second):
		t.Fail()
	case <-done:
	}
}

func TestProduceStructMessage(t *testing.T) {
	ctx := context.Background()
	dq := New(&Options{
		Name:     "test",
		RedisOpt: testRdb,
	})

	type ss struct {
		Field1 string
		Field2 string
	}

	dq.Consume(ctx, HandlerFunc(func(ctx context.Context, m *Message) error {
		var s ss
		err := m.GetSchemaValue(&s)
		bs, _ := json.Marshal(s)
		fmt.Println(string(bs), err)
		return nil
	}))

	for i := 0; i < 10; i++ {
		_, _ = dq.Produce(context.Background(), &ProducerMessage{
			Value: ss{Field1: strconv.Itoa(i), Field2: strconv.Itoa(i + 1)},
		})
	}

	select {}
}
