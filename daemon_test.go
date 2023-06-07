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

	New(options...)

	<-time.Tick(10 * time.Second)
}

func TestDaemonDelayToReady(t *testing.T) {
	// TODO: fix this test
	t.SkipNow()

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
		t.Logf("consume: %s %s", m.ID, m.Payload)
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
