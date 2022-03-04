package main

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/9ft/dqueue"
)

func main() {
	ctx := context.Background()

	dq := dqueue.New(&dqueue.Options{
		Name:                 "test",
		RedisOpt:             &redis.Options{Addr: "127.0.0.1:6379"},
		DaemonWorkerInterval: 1 * time.Second,
		EnableCancel:         true,
	})

	dq.Consume(ctx, dqueue.HandlerFunc(func(ctx context.Context, m *dqueue.Message) error {
		var i int
		_ = m.GetSchemaValue(&i)
		log.Println("consume:", i)
		return nil
	}))

	for i := 0; i < 10; i++ {
		id, _ := dq.Produce(ctx, &dqueue.ProducerMessage{
			Value: i,
		})
		log.Println("produce:", id)
	}

	select {}
}
