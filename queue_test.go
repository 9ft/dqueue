package dq

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkProduceReady(b *testing.B) {
	ctx := context.Background()

	q := New()
	defer b.Cleanup(func() { cleanup(nil, q) })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i))})
		assert.Nil(b, err)
	}
	b.StopTimer()
}

func BenchmarkProduceDelay(b *testing.B) {
	ctx := context.Background()

	q := New()
	defer b.Cleanup(func() { cleanup(nil, q) })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		at := time.Now().Add(1 * time.Second)
		_, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("payload" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(b, err)
	}
	b.StopTimer()
}

func BenchmarkConsumeRealtime(b *testing.B) {
	ctx := context.Background()

	q := New(options...)
	defer b.Cleanup(func() { cleanup(nil, q) })

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
}

func BenchmarkConsumeDelay(b *testing.B) {
	ctx := context.Background()

	fmt.Println("B.N: ", b.N)

	q := New(options...)
	defer b.Cleanup(func() { cleanup(nil, q) })

	var wg sync.WaitGroup
	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		at := time.Now().Add(1 * time.Second)
		_, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload" + strconv.Itoa(i)), At: &at})
		assert.Nil(b, err)
	}

	<-time.After(1 * time.Second)

	b.ResetTimer()
	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		wg.Done()
		return nil
	})
	wg.Wait()
	b.StopTimer()
	fmt.Println("done")
}
