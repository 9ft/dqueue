package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/mzcabc/dq"
)

func main() {
	ctx := context.Background()

	q := dq.New()

	q.Consume(dq.HandlerFunc(func(ctx context.Context, m *dq.Message) error {
		bs, _ := json.Marshal(m)
		fmt.Println(time.Now(), "consume message:", string(bs))
		return nil
	}))

	// produce realtime message
	for i := 0; i < 10; i++ {
		id, err := q.Produce(ctx, &dq.ProducerMessage{
			Payload: []byte("realtime message, i =" + strconv.Itoa(i)),
		})
		fmt.Println(time.Now(), "produce realtime message:", id, "err:", err)
	}

	// produce delay message
	for i := 0; i < 10; i++ {
		at := time.Now().Add(3 * time.Second)
		id, err := q.Produce(ctx, &dq.ProducerMessage{
			Payload:   []byte("delay message, i =" + strconv.Itoa(i)),
			DeliverAt: &at,
		})
		fmt.Println(time.Now(), "produce delay message:", id, "err:", err)
	}

	<-time.After(10 * time.Second)
}
