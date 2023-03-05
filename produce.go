package dq

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ProducerMessage struct {
	Payload []byte
	At      *time.Time
}

func (q *Queue) Produce(ctx context.Context, m *ProducerMessage) (id string, err error) {
	if m.Payload == nil {
		return "", fmt.Errorf("payload is nil")
	}

	id = uuid.NewString()
	err = q.enqueue(ctx,
		id,
		base64.StdEncoding.EncodeToString(m.Payload),
		time.Now(),
		m.At)
	if err != nil {
		return "", fmt.Errorf("enqueue failed, err: %v", err)
	}

	return id, nil
}

var zeroTime = time.Unix(0, 0)

func (q *Queue) enqueue(ctx context.Context, id, msg string, createAt time.Time, deliverAt *time.Time) error {
	if deliverAt == nil {
		deliverAt = &zeroTime
	}
	if deliverAt == nil || deliverAt.Before(createAt) {
		return q.lPushSetEx(ctx, q.getKey(kReady), q.getKey(kMsg), id, msg, createAt, *deliverAt, int(q.messageSaveTime.Seconds()))
	}
	return q.zAddSetEx(ctx, q.getKey(kDelay), q.getKey(kMsg), id, msg, createAt, deliverAt, int(q.messageSaveTime.Seconds()))
}
