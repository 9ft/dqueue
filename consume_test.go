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

func TestConsume(t *testing.T) {
	t.SkipNow()

	q := New(WithLogMode(Trace), WithConsumerWorkerNum(1), WithConsumerWorkerInterval(5*time.Second))

	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Log("consume:", m.ID)
		return nil
	}))

	<-time.Tick(100 * time.Second)
}

func TestMultiInstance(t *testing.T) {
	ctx := context.Background()
	// init
	q := New()
	q1 := New()
	q2 := New()

	// produce
	num := 10000
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

	q1.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		// t.Log("q1 consume:", m.ID)
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return nil
	}))
	q2.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		// t.Log("q2 consume:", m.ID)
		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return nil
	}))

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// check
	assert.ElementsMatch(t, sendIDs, recvIDs)

	// clean
	delCnt, err := q.rdb.XDel(ctx, q.getKey(kReady), recvIDs...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}

func TestProduceConsumeRealtime(t *testing.T) {
	ctx := context.Background()
	// init
	q := New()

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		t.Log("produced:", id)
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
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume_payload:", string(m.Payload))

		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return nil
	}))

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// check
	assert.Equal(t, sendIDs, recvIDs)

	// clean
	delCnt, err := q.rdb.XDel(ctx, q.getKey(kReady), recvIDs...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}

func TestProduceConsumeRealtimeWithErrRetry(t *testing.T) {
	// init
	q := New()

	// produce
	num := 10
	var sendIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		t.Log("produced:", id)
		assert.Nil(t, err)
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
	q.Consume(context.Background(), HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume_payload:", string(m.Payload))

		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return fmt.Errorf("fake err")
	}))

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// check
	assert.Equal(t, append(append(sendIDs, sendIDs...), sendIDs...), recvIDs)

	// clean
	delCnt, err := q.rdb.XDel(context.Background(), q.getKey(kReady), sendIDs...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}

func TestProduceConsumeDelay(t *testing.T) {
	ctx := context.Background()

	// init
	q := New()

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
		t.Log("produced:", id)
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
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Logf("consume: %s, %s", m.ID, m.Payload)

		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return fmt.Errorf("fake err")
	}))

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// check
	assert.ElementsMatch(t, sendIDs, recvIDs)

	keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
	assert.Nil(t, err)
	assert.Equal(t, num, len(keys))

	// clean
	delCnt, err := q.rdb.Del(ctx, keys...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)

	delCnt, err = q.rdb.Del(ctx, q.getKey(kReady)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, delCnt)
}

func TestProduceConsumeDelayWithErrRetry(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(WithRetryInterval(1*time.Second), WithRetryTimes(3))

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
		t.Log("produced:", id)
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
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		bs, _ := json.Marshal(m)
		t.Log("consume:", string(bs))
		t.Log("consume_payload:", string(m.Payload))

		recvIDsMu.Lock()
		recvIDs = append(recvIDs, m.ID)
		recvIDsMu.Unlock()
		wg.Done()
		return fmt.Errorf("fake err")
	}))

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// check
	assert.ElementsMatch(t, append(append(sendIDs, sendIDs...), sendIDs...), recvIDs)

	keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
	assert.Nil(t, err)
	assert.Equal(t, num, len(keys))

	// clean
	delCnt, err := q.rdb.Del(ctx, keys...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)

	delCnt, err = q.rdb.Del(ctx, q.getKey(kReady)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, delCnt)
}

func TestGracefulShutdown(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(
		WithConsumerTakeBlockTimeout(10*time.Millisecond),
		WithConsumerWorkerInterval(10*time.Millisecond),
		WithConsumerWorkerNum(10),
		WithLogMode(Trace))

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
	<-time.Tick(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()
	t.Log("graceful shutdown")
	assert.Nil(t, q.Close(ctx))
}

func TestGracefulShutdownWithError(t *testing.T) {
	// init
	q := New(
		WithConsumerTakeBlockTimeout(10*time.Millisecond),
		WithConsumerWorkerInterval(10*time.Millisecond),
		WithConsumerWorkerNum(10),
		WithLogMode(Trace))
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

	<-time.Tick(50 * time.Millisecond)

	// graceful shutdown timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	t.Log("graceful shutdown")
	assert.ErrorIs(t, q.Close(ctx), context.DeadlineExceeded)

	// wait for shutdown complete
	<-time.Tick(100 * time.Millisecond)

	// clean
	New().Consume(context.Background(),
		HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
			t.Log("clean:", m.ID)
			return nil
		}))
	<-time.After(1 * time.Second)
}

func TestCancelDelay(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(WithConsumerTakeBlockTimeout(10 * time.Millisecond))

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
		t.Log("produced:", id)
		sendIDs = append(sendIDs, id)
	}

	// cancel
	for _, id := range sendIDs {
		assert.Nil(t, q.Cancel(ctx, id))
	}

	// consume
	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Fatalf("consume cancelled message: %s, %s", m.ID, m.Payload)
		return nil
	}))

	<-time.Tick(1 * time.Second)

	// check
	keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))

	// clean
	delCnt, err := q.rdb.Del(ctx, q.getKey(kReady)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, delCnt)
}
