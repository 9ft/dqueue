package dq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var options []func(*Queue)

func TestMain(m *testing.M) {
	options = []func(*Queue){
		WithConsumerTakeBlockTimeout(100 * time.Millisecond),
		WithConsumerWorkerInterval(10 * time.Millisecond),
		WithConsumerWorkerNum(10),

		WithRetryTimes(3),
		WithRetryInterval(1000 * time.Millisecond),

		WithDaemonWorkerInterval(100 * time.Millisecond),
		WithDaemonWorkerNum(5),
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}

func cleanup(t *testing.T, qs ...*Queue) {
	ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()

	var wg sync.WaitGroup
	wg.Add(len(qs))

	for _, q := range qs {
		go func(q *Queue) {
			defer wg.Done()

			assert.Nil(t, q.Destroy(ctx))

			keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
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

func TestConsume(t *testing.T) {
	t.SkipNow()

	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consume:", m.ID)
		return nil
	})

	<-time.Tick(100 * time.Second)
}

func TestMultiInstance(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	q1 := New(options...)
	q2 := New(options...)

	defer t.Cleanup(func() { cleanup(t, q, q1, q2) })

	// produce
	num := 1000
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		// t.Log("produced:", id)
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var (
		recvIDs   []string
		recvIDsMu sync.Mutex

		done = make(chan struct{})
		wg   sync.WaitGroup
	)

	go func() {
		wg.Wait()
		close(done)
	}()

	wg.Add(num)
	handler := func(name string) HandlerFunc {
		return func(ctx context.Context, m *ConsumerMessage) error {
			// t.Log(name, "consume:", m.ID)
			recvIDsMu.Lock()
			recvIDs = append(recvIDs, m.ID)
			recvIDsMu.Unlock()
			wg.Done()
			return nil
		}
	}

	q1.Consume(context.Background(), handler("q1"))
	q2.Consume(context.Background(), handler("q2"))

	// wait
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	case <-done:
		t.Log("consume done, wait 1s to check")
		<-time.After(1 * time.Second)
	}

	// check
	assert.ElementsMatch(t, sendIDs, recvIDs)
}

func TestProduceConsumeRealtime(t *testing.T) {
	ctx := context.Background()
	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 1000
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		// t.Log("produced:", id)
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

	var (
		recvIDs   = []string{}
		recvIDsMu sync.Mutex
	)
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		// t.Log("consume:", m.ID)
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return nil
	})

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
		t.Log("consume done, wait 1s to check")
		<-time.After(1 * time.Second)
	}

	// check
	assert.ElementsMatch(t, sendIDs, recvIDs)
}

func TestProduceConsumeRealtimeWithErrRetry(t *testing.T) {
	// init
	retryTimes := 3
	q := New(append(options,
		WithRetryTimes(retryTimes),
	)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		// t.Log("produced:", id)
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num * retryTimes)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var (
		recvIDs   = []string{}
		recvIDsMu sync.Mutex
	)
	q.Consume(context.Background(), func(ctx context.Context, m *ConsumerMessage) error {
		// t.Log(time.Now(), "consume:", m.ID)
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return fmt.Errorf("fake err")
	})

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
		t.Log("consume done, wait 3s to check")
		<-time.After(3 * time.Second)
	}

	// check
	var want []string
	for i := 0; i < retryTimes; i, want = i+1, append(want, sendIDs...) {
	}
	assert.ElementsMatch(t, want, recvIDs)
}

func TestProduceConsumeDelay(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(100 * time.Millisecond)
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		// t.Log("produced:", id)
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

	var (
		recvIDs   = []string{}
		recvIDsMu sync.Mutex
	)
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		// t.Logf("consume: %s, %s", m.ID, m.Payload)
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return nil
	})

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
		t.Log("consume done, wait 3s to check")
		<-time.After(3 * time.Second)
	}

	// check
	assert.ElementsMatch(t, sendIDs, recvIDs)
}

func TestProduceConsumeDelayWithErrRetry(t *testing.T) {
	ctx := context.Background()

	// init
	retryTimes := 3
	q := New(append(options,
		WithRetryTimes(retryTimes))...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(100 * time.Millisecond)
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		// t.Log("produced:", id)
		sendIDs = append(sendIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num * 3)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var (
		recvIDs   = []string{}
		recvIDsMu sync.Mutex
	)
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return fmt.Errorf("fake err")
	})

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
		t.Log("consume done, wait 3s to check")
		<-time.After(3 * time.Second)
	}

	// check
	want := []string{}
	for i := 0; i < retryTimes; i, want = i+1, append(want, sendIDs...) {
	}
	assert.ElementsMatch(t, want, recvIDs)
}

func TestGracefulShutdown(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// consume
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consumer begin process:", m.ID)
		<-time.After(100 * time.Millisecond)
		t.Log("consumer end process:", m.ID)
		return nil
	})

	// produce
	id, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload")})
	assert.Nil(t, err)
	t.Log("produce:", id)

	// graceful shutdown success
	<-time.Tick(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()
	t.Log("graceful shutdown")
	assert.Nil(t, q.Close(ctx))
}

func TestGracefulShutdownWithError(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// consume
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consumer begin process:", m.ID)
		<-time.After(100 * time.Millisecond)
		t.Log("consumer end process:", m.ID)
		return nil
	})

	// produce
	id, err := q.Produce(ctx, &ProducerMessage{Payload: []byte("payload")})
	assert.Nil(t, err)
	t.Log("produce:", id)

	<-time.Tick(50 * time.Millisecond)

	// graceful shutdown timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	t.Log("graceful shutdown")
	assert.ErrorIs(t, q.Close(ctx), context.DeadlineExceeded)

	// wait for shutdown complete
	<-time.Tick(100 * time.Millisecond)
}

func TestCancelDelay(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(100 * time.Millisecond)
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		// t.Log("produced:", id)
		sendIDs = append(sendIDs, id)
	}

	// cancel
	for _, id := range sendIDs {
		assert.Nil(t, q.Cancel(ctx, id))
	}

	// consume
	q.Consume(ctx, func(ctx context.Context, m *ConsumerMessage) error {
		t.Fatalf("consume cancelled message: %s, %s", m.ID, m.Payload)
		return nil
	})

	<-time.Tick(1 * time.Second)
}
