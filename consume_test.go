package dq

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsume(t *testing.T) {
	t.SkipNow()

	q := New()

	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consume:", m.ID)
		return nil
	}))

	<-time.Tick(10 * time.Second)
}

func TestGracefulShutdown(t *testing.T) {
	// init
	q := New(WithConsumerWorkerNum(10))
	ctx := context.Background()

	// consume
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
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
	q := New(WithConsumerWorkerNum(10))
	ctx := context.Background()

	// consume
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consumer begin process:", m.ID)
		<-time.After(100 * time.Millisecond)
		t.Log("consumer end process:", m.ID)
		return nil
	}))

	// produce
	id, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload")})
	assert.Nil(t, err)
	t.Log("produce:", id)

	// graceful shutdown timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, q.Close(ctx), context.DeadlineExceeded)

	// clean
	New(WithName("test")).Consume(context.Background(),
		HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
			t.Log("clean:", m.ID)
			return nil
		}))
	<-time.After(1 * time.Second)
}

func TestProduceConsumeRealtime(t *testing.T) {
	// init
	q := New()

	// produce
	num := 5
	var msgKeys []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		assert.Nil(t, err)
		msgKeys = append(msgKeys, q.getKey(kMsg)+":"+id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume:", string(m.Payload))

		wg.Done()
		return nil
	}))

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}

func TestProduceConsumeDelay(t *testing.T) {
	// init
	q := New()

	// produce
	num := 5
	var msgKeys []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(100 * time.Millisecond)
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		msgKeys = append(msgKeys, q.getKey(kMsg)+":"+id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume:", string(m.Payload))

		wg.Done()
		return nil
	}))

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}
