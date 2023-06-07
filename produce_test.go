package dq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProduceReady(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	// produce
	num := 100
	var msgIDs []string
	for i := 0; i < num; i++ {
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("ready_" + strconv.Itoa(i)),
		})
		// t.Logf("message produced: %s", id)
		assert.Nil(t, err)
		msgIDs = append(msgIDs, id)
	}

	// assert
	assert.Equal(t, num, len(msgIDs))

	cnt, err := q.rdb.XLen(ctx, q.getKey(kReady)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, cnt)
}

func TestProduceDelay(t *testing.T) {
	ctx := context.Background()

	// init
	q := New(options...)
	defer t.Cleanup(func() { cleanup(t, q) })

	num := 10
	var msgIDs []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(1 * time.Second)
		id, err := q.Produce(ctx, &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		t.Logf("message produced: %s", id)
		assert.Nil(t, err)
		msgIDs = append(msgIDs, id)
	}

	// assert
	assert.Equal(t, num, len(msgIDs))

	dbCnt, err := q.rdb.ZCard(ctx, q.getKey(kDelay)).Result()
	assert.Nil(t, err)
	assert.EqualValues(t, num, dbCnt)

	keys, err := q.rdb.Keys(ctx, q.getKey(kMsg)+":*").Result()
	assert.Nil(t, err)
	assert.Equal(t, num, len(keys))
}
