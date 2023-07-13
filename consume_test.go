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

func TestConsume(t *testing.T) {
	t.SkipNow()

	q := New(append(testOpts(t), WithName(""))...)

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Log("consume:", m.ID)
		return nil
	}))

	<-time.Tick(10 * time.Second)
}

func TestConsumeRealtime(t *testing.T) {
	// init
	q := New(testOpts(t)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 5
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		// t.Log("consume:", string(m.Payload))
		wg.Done()
		return nil
	}))

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}

func TestConsumeDelay(t *testing.T) {
	// init
	q := New(testOpts(t)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 5
	var sendIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(100 * time.Millisecond)
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload:   []byte("delay_" + strconv.Itoa(i)),
			DeliverAt: &at,
		})
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		// t.Log("consume:", string(m.Payload))
		wg.Done()
		return nil
	}))

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}

func TestConsumeErrRetry(t *testing.T) {
	// init
	retry := 3
	q := New(append(testOpts(t),
		WithRetryTimes(retry),
		WithRetryInterval(10*time.Millisecond),
	)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num * (retry + 1))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Log("consume:", m.DeliverCnt, string(m.Payload))
		wg.Done()
		return fmt.Errorf("mock err")
	}))

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}

func TestConsumePanicRetry(t *testing.T) {
	// init
	retry := 3
	q := New(append(testOpts(t),
		WithRetryTimes(retry),
		WithRetryInterval(10*time.Millisecond),
	)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 5
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num * (retry + 1))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Log("consume:", m.DeliverCnt, string(m.Payload))
		wg.Done()
		panic("mock panic")
		return nil
	}))

	select {
	case <-time.After(200 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}

func TestGracefulShutdown(t *testing.T) {
	// init
	q := New(append(testOpts(t),
		WithRetryInterval(10*time.Millisecond),
		WithConsumerWorkerInterval(10*time.Millisecond),
		WithDaemonWorkerInterval(10*time.Millisecond),
		WithLogMode(Trace),
	)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	ctx := context.Background()

	// consume
	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Log("consumer begin process:", m.ID)
		<-time.After(100 * time.Millisecond)
		t.Log("consumer end process:", m.ID)
		return nil
	}))

	// produce
	id, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload")})
	assert.Nil(t, err)
	t.Log("produce:", id)

	// graceful shutdown success
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.Nil(t, q.Close(ctx))
}

func TestGracefulShutdownWithError(t *testing.T) {
	// init
	q := New(append(testOpts(t),
		WithRetryInterval(10*time.Millisecond),
		WithConsumerWorkerInterval(10*time.Millisecond),
		WithDaemonWorkerInterval(10*time.Millisecond),
		WithLogMode(Trace),
	)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	ctx := context.Background()

	// consume
	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Log("consumer begin process:", m.ID)
		<-time.After(1000 * time.Millisecond)
		t.Log("consumer end process:", m.ID)
		return nil
	}))

	// produce
	id, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload")})
	assert.Nil(t, err)
	t.Log("produce:", id)

	<-time.After(10 * time.Millisecond)

	// graceful shutdown timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, q.Close(ctx), context.DeadlineExceeded)
}
