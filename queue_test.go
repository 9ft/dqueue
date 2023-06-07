package dq

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkProduceReady(b *testing.B) {
	q := New()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i))})
		assert.Nil(b, err)
	}
	b.StopTimer()

	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		wg.Done()
		return nil
	})

	wg.Wait()
}

func BenchmarkProduceDelay(b *testing.B) {
	q := New()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(b.N)

	at := time.Now().Add(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("payload" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(b, err)
	}
	b.StopTimer()

	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		wg.Done()
		return nil
	})

	wg.Wait()
}

func BenchmarkConsume(b *testing.B) {
	q := New(
		WithConsumerWorkerNum(4),
		WithConsumerWorkerInterval(1*time.Millisecond),
	)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i))})
		assert.Nil(b, err)
	}

	b.ResetTimer()
	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		wg.Done()
		return nil
	})
	wg.Wait()
	b.StopTimer()

	_ = q.Close(context.Background())

	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		b.FailNow()
		return nil
	})

	<-time.After(1 * time.Second)
}
