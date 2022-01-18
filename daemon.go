package dqueue

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

func (d *DQueue) daemon() {
	for i := 0; i < d.daemonWorkerNum; i++ {
		go func() {
			ticker := time.NewTicker(d.daemonWorkerInterval)
			for {
				go delayToReadySingle(d.rdb, time.Now(), d.name)
				<-ticker.C
			}
		}()
	}
}

func delayToReadySingle(rdb *redis.Client, t time.Time, name string) error {
	dq := delayQueuePrefix + name
	rq := readyQueuePrefix + name

	script := redis.NewScript(`
local message = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, 1);
if #message > 0 then
  redis.call('ZREM', KEYS[1], message[1]);
  redis.call('RPUSH', KEYS[2], message[1]);
  return message;
else
  return {};
end
`)

	_, err := script.Run(context.Background(), rdb, []string{dq, rq}, t.UnixMilli()).StringSlice() // [member, score]
	return err
}
