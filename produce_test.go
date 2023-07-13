package dq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProduceReady(t *testing.T) {
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

	// assert
	assert.Equal(t, num, len(sendIDs))
}

func TestProduceDelay(t *testing.T) {
	// init
	q := New(testOpts(t)...)
	defer t.Cleanup(func() { cleanup(t, q) })

	num := 5
	var sendIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(1 * time.Second)
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload:   []byte("delay_" + strconv.Itoa(i)),
			DeliverAt: &at,
		})
		assert.Nil(t, err)
		sendIDs = append(sendIDs, id)
	}

	// assert
	assert.Equal(t, num, len(sendIDs))
}
