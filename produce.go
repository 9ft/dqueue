package dq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type ProducerMessage struct {
	Payload []byte
	At      *time.Time
}

func (q *Queue) Produce(ctx context.Context, m *ProducerMessage) (id string, err error) {
	if m.Payload == nil {
		return "", fmt.Errorf("payload is nil")
	}

	id, err = q.enqueue(ctx,
		base64.StdEncoding.EncodeToString(m.Payload),
		time.Now(),
		m.At)
	if err != nil {
		return "", fmt.Errorf("enqueue failed, err: %v", err)
	}

	return id, nil
}

var zeroTime = time.Unix(0, 0)

func (q *Queue) enqueue(ctx context.Context, msg string, createAt time.Time, deliverAt *time.Time) (string, error) {
	if deliverAt == nil {
		deliverAt = &zeroTime
	}

	if deliverAt.Before(createAt) {
		return q.enqueueReady(ctx, msg, createAt)
	}
	return q.enqueueDelay(ctx, msg, createAt, deliverAt)
}

func (q *Queue) enqueueReady(ctx context.Context, msg string, createAt time.Time) (string, error) {
	id, err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.key(kReady),
		// NoMkStream: true,
		MaxLen: q.streamMaxLen,
		Approx: true,
		ID:     "*",
		Values: map[string]interface{}{
			// TODO struct
			"ctime": createAt.UnixMilli(),
			"msg":   msg,
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd failed, err: %v", err)
	}
	return id, nil
}

func (q *Queue) enqueueDelay(ctx context.Context, msg string, createAt time.Time, deliverAt *time.Time) (string, error) {
	// TODO struct
	msgData := map[string]interface{}{
		"ctime": fmt.Sprint(createAt.UnixMilli()),
		"dtime": fmt.Sprint(deliverAt.UnixMilli()),
		"msg":   msg,
	}
	bs, _ := json.Marshal(msgData)

	id := uuid.NewString()
	return id, q.zAddSetEx(ctx, q.key(kDelay), q.key(kMsg), id, string(bs), deliverAt, int(q.messageSaveTime.Seconds()))
}

func (q *Queue) Cancel(ctx context.Context, id string) error {
	_, err := q.rdb.commit(ctx, q.key(kDelay), q.key(kMsg), id)
	if err != nil {
		return fmt.Errorf("commit message failed, err: %v", err)
	}
	return nil
}
