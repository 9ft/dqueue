package dq

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type ProducerMessage struct {
	Payload   []byte
	DeliverAt *time.Time
}

type Message struct {
	ProducerMessage

	ID         string
	DeliverCnt int
	CreateAt   time.Time
}

func (m *Message) values() []interface{} {
	values := []interface{}{
		"id", m.ID,
		"payload", m.Payload,
		"create_at", m.CreateAt.UnixMilli(),
	}
	if m.DeliverAt != nil {
		values = append(values, "deliver_at", m.DeliverAt.UnixMilli())
	}

	return values
}

func (m *Message) parse(values []string) error {
	if m == nil {
		return errors.New("message is nil")
	}

	if len(values)&1 != 0 {
		return errors.New("invalid values")
	}

	for i := 0; i < len(values); i += 2 {
		switch values[i] {
		case "id":
			m.ID = values[i+1]
		case "payload":
			bs, err := base64.StdEncoding.DecodeString(values[i+1])
			if err != nil {
				return fmt.Errorf("base64 decode failed, str: %s, err: %v", values[i+1], err)
			}
			m.Payload = bs
		case "create_at":
			i, _ := strconv.ParseInt(values[i+1], 10, 64)
			m.CreateAt = time.UnixMilli(i)
		case "deliver_at":
			i, _ := strconv.ParseInt(values[i+1], 10, 64)
			t := time.UnixMilli(i)
			m.DeliverAt = &t
		case "deliver_cnt":
			i, _ := strconv.ParseInt(values[i+1], 10, 64)
			m.DeliverCnt = int(i)
		}
	}
	return nil
}
