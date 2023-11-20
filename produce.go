package dq

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (q *Queue) Produce(ctx context.Context, m *ProducerMessage) (id string, err error) {
	defer func() {
		if q.opts.metric != nil {
			go q.opts.metric.Produce(m.DeliverAt != nil, err)
		}
	}()
	if m.Payload == nil {
		return "", fmt.Errorf("payload is nil")
	}

	id = uuid.NewString()
	err = q.enqueue(ctx,
		id,
		base64.StdEncoding.EncodeToString(m.Payload),
		time.Now(),
		m.DeliverAt)
	if err != nil {
		return "", fmt.Errorf("enqueue failed, err: %v", err)
	}

	return id, nil
}

func (q *Queue) enqueue(ctx context.Context, id, msg string, createAt time.Time, deliverAt *time.Time) error {
	cm := &Message{
		ProducerMessage: ProducerMessage{
			Payload:   []byte(msg),
			DeliverAt: deliverAt,
		},

		ID:       id,
		CreateAt: time.Now(),
	}

	if deliverAt == nil || deliverAt.Before(createAt) {
		// realtime message
		return q.runProduceRealtimeMsg(ctx, q.key(kReady), q.key(kData), cm, int(q.messageSaveTime.Seconds()))
	}

	// delay message
	return q.runProduceDelayMsg(ctx, q.key(kDelay), q.key(kData), cm, int(q.messageSaveTime.Seconds()))
}

func (q *Queue) Cancel(ctx context.Context, id string) error {
	_, err := q.rdb.Del(ctx, q.key(kData)+":"+id).Result()
	if err != nil {
		return fmt.Errorf("del message failed, err: %v", err)
	}
	return nil
}
