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

	q.daemon(context.Background())

	<-time.Tick(10 * time.Second)
}

func TestDaemonDelayToReady(t *testing.T) {
	// init
	q := New(testOpts(t)...)

	// produce
	num := 10
	at := time.Now().Add(10 * time.Millisecond)
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload:   []byte("delay_" + strconv.Itoa(i)),
			DeliverAt: &at,
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

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
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
	retry := 3
	q := New(append(testOpts(t),
		WithRetryTimes(retry),
		WithRetryInterval(10*time.Millisecond),
	)...)

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

	q.Consume(HandlerFunc(func(ctx context.Context, m *Message) error {
		t.Logf("consume: %s, retried: %d", m.ID, m.DeliverCnt)
		wg.Done()
		if m.DeliverCnt == 1 {
			return fmt.Errorf("retry mock")
		}
		return nil
	}))

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}
}
