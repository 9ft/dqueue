// Package dqueue defines message queues, supporting real-time and delayed message
// Use redis list as ready queue. Use redis sorted set as delay queue.
package dqueue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var testRdb = &redis.Options{Addr: "127.0.0.1:6379"}

func TestMain(m *testing.M) {
	fmt.Println("TestMain before Run")
	exitCode := m.Run()
	fmt.Println("TestMain after Run")

	// 退出
	os.Exit(exitCode)
}

func TestPrintABC(t *testing.T) {
	fmt.Println("ABC")
}

func TestDQProduceAndConsume(t *testing.T) {
	ctx := context.Background()
	dq := New(&Options{
		Name:     "test",
		RedisOpt: testRdb,
	})

	dq.Consume(ctx, func(message *Message) {
		ii := message.Payload.AsInterface().(float64)

		fmt.Println("recv:", ii)

		if ii > 15 && message.Retried < 1 {
			dq.RedeliveryAfter(context.Background(), message, 3*time.Second)
		}
	})

	for i := 0; i < 10; i++ {
		_, _ = dq.Produce(context.Background(), &ProducerMessage{Value: i})
		fmt.Println("produce:", i)
	}

	for i := 10; i < 20; i++ {
		t := time.Now().Add(3 * time.Second)
		_, _ = dq.Produce(context.Background(), &ProducerMessage{Value: i, DeliverAt: t})
		fmt.Println("produce delay:", i)
	}

	time.Sleep(5 * time.Second)

	// dq.Close()

	// dq.Destroy()

	select {}

}
