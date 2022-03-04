package dqueue

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

func (q *queue) daemon() {
	for i := 0; i < q.daemonWorkerNum; i++ {
		go func() {
			if q.daemonWorkerInterval == 0 {
				for {
					delayToReady(q.rdb.c, time.Now(), q.name)
				}
			}

			ticker := time.NewTicker(q.daemonWorkerInterval)
			for {
				go delayToReady(q.rdb.c, time.Now(), q.name)
				<-ticker.C
			}
		}()
	}
}

func delayToReady(rdb *redis.Client, t time.Time, name string) (int, error) {
	dq := delayPrefix + name
	rq := readyPrefix + name

	script := redis.NewScript(`
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, 1000);
if #members > 0 then
  for key, value in ipairs(members)
  do
    redis.call('ZREM', KEYS[1], value);
    redis.call('RPUSH', KEYS[2], value);
  end
  return #members;
else
  return 0;
end
`)

	return script.Run(context.Background(), rdb, []string{dq, rq}, t.UnixMilli()).Int()
}
