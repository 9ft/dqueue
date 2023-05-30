package dq

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDaemon(t *testing.T) {
	t.SkipNow()

	New(WithDaemonWorkerNum(1), WithDaemonWorkerInterval(3*time.Second), WithLogMode(Trace))

	<-time.Tick(10 * time.Second)
}

func TestDaemonDelayToReady(t *testing.T) {
	ctx := context.Background()
	// init
	q := New()

	// produce
	num := 10
	var msgIDs []string
	at := time.Now().Add(100 * time.Millisecond)
	for i := 0; i < num; i++ {
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		t.Logf("message produced: %s", id)
		msgIDs = append(msgIDs, id)
	}

	// consume
	var wg sync.WaitGroup
	wg.Add(num)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	q.Consume(ctx, HandlerFunc(func(ctx context.Context, m *ConsumerMessage) error {
		t.Logf("consume: %#v, %s, %s\n", m, m.ID, string(m.Payload))
		wg.Done()
		return nil
	}))

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("consume timeout")
	case <-done:
	}

	// assert
	keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
	assert.Nil(t, err)
	assert.Equal(t, num, len(keys))

	// clean
	delCnt, err := q.rdb.Del(ctx, keys...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)

	messages, err := q.rdb.XRangeN(ctx, q.getKey(kReady), "-", "+", int64(num)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, len(messages))
	var ids []string
	for _, m := range messages {
		ids = append(ids, m.ID)
	}

	delCnt, err = q.rdb.XDel(ctx, q.getKey(kReady), ids...).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}
