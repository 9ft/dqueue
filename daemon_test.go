package dq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDaemon(t *testing.T) {
	t.SkipNow()

	q := New()

	q.daemon()

	<-time.Tick(10 * time.Second)
}

func TestDaemonDelayToReady(t *testing.T) {
	// init
	q := New()

	// produce
	num := 10
	at := time.Now().Add(10 * time.Millisecond)
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		t.Log("produce:", id)
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

func TestDaemonRetryToReady(t *testing.T) {
	// init
	q := New(WithRetryInterval(100 * time.Millisecond))

	// produce
	num := 10
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		assert.Nil(t, err)
		t.Log("produce:", id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num * 2)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume:", string(m.Payload))

		if m.Retried == 0 {
			return fmt.Errorf("retry mock")
		}

		wg.Done()
		return nil
	}))

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("consume timeout")
	case <-done:
	}
}
