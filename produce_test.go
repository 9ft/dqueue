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

	// assert
	assert.Equal(t, num, len(msgKeys))

	dbCnt, err := q.rdb.LLen(context.Background(), q.getKey(kReady)).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, num, dbCnt)

	// clean
	delCnt, err := q.rdb.Del(context.Background(), q.getKey(kReady)).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, delCnt)

	delCnt, err = q.rdb.Del(context.Background(), msgKeys...).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}

func TestProduceDelay(t *testing.T) {
	q := New()

	num := 100
	var msgKeys []string
	for i := 0; i < num; i++ {
		at := time.Now().Add(1 * time.Second)
		id, err := q.Produce(context.Background(), &ProducerMessage{
			Payload: []byte("delay_" + strconv.Itoa(i)),
			At:      &at,
		})
		assert.Nil(t, err)
		msgKeys = append(msgKeys, q.getKey(kMsg)+":"+id)
	}

	// assert
	assert.Equal(t, num, len(msgKeys))

	dbCnt, err := q.rdb.ZCard(context.Background(), q.getKey(kDelay)).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, num, dbCnt)

	// clean
	delCnt, err := q.rdb.Del(context.Background(), q.getKey(kDelay)).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, delCnt)

	delCnt, err = q.rdb.Del(context.Background(), msgKeys...).Uint64()
	assert.Nil(t, err)
	assert.EqualValues(t, num, delCnt)
}
