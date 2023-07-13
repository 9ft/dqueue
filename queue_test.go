package dq

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testOpts(t *testing.T) []func(*Queue) {
	name := "dq_test_"
	if t != nil {
		name += t.Name()
	}
	return []func(*Queue){
		WithName(name),
		WithConsumerWorkerNum(10),
		WithMessageSaveTime(1 * time.Minute),
	}
}

func cleanup(t *testing.T, qs ...*Queue) {
	ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()

	var wg sync.WaitGroup
	wg.Add(len(qs))

	for _, q := range qs {
		go func(q *Queue) {
			defer wg.Done()

			keys, err := q.rdb.Keys(ctx, q.redisPrefix+q.name+":*").Result()
			assert.Nil(t, err)
			if len(keys) == 0 {
				return
			}

			_, err = q.rdb.Del(ctx, keys...).Result()
			assert.Nil(t, err)
		}(q)
	}
	wg.Wait()
}

var benchOnce = sync.Once{}
var benchWg sync.WaitGroup

func BenchmarkProduceReady(b *testing.B) {
	q := New(append(testOpts(nil), WithRetryInterval(1*time.Second), WithLogMode(Trace))...)
	ctx := context.Background()

	cnt := b.N
	benchWg.Add(cnt)

	b.ResetTimer()
	for i := 0; i < cnt; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i))})
		assert.Nil(b, err)
	}
	b.StopTimer()

	benchOnce.Do(func() {
		q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
			benchWg.Done()
			return nil
		}))
	})

	benchWg.Wait()
}

func BenchmarkProduceDelay(b *testing.B) {
	q := New()
	ctx := context.Background()

	cnt := b.N
	benchWg.Add(cnt)

	at := time.Now().Add(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < cnt; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{
			Payload:   []byte("payload" + strconv.Itoa(i)),
			DeliverAt: &at,
		})
		assert.Nil(b, err)
	}
	b.StopTimer()

	benchOnce.Do(func() {
		q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
			benchWg.Done()
			return nil
		}))
	})
	benchWg.Wait()
}

func BenchmarkConsume(b *testing.B) {
	q := New()
	ctx := context.Background()

	cnt := b.N
	benchWg.Add(cnt)

	for i := 0; i < cnt; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i))})
		assert.Nil(b, err)
	}
	benchOnce.Do(func() {
		q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
			benchWg.Done()
			return nil
		}))
	})
	benchWg.Wait()
}
