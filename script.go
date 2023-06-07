package dq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var scriptZAddSetEx = redis.NewScript(`
redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), ARGV[1])
redis.call('SET', KEYS[2] .. ':' .. ARGV[1], ARGV[2], 'EX', ARGV[4])`)

func (r *rdb) zAddSetEx(ctx context.Context, zset, data, id, msg string, deliverAt *time.Time, expSec int) error {
	err := scriptZAddSetEx.Run(ctx, r, []string{zset, data}, id, msg, deliverAt.UnixMilli(), expSec).Err()
	if err != redis.Nil && err != nil {
		return fmt.Errorf("zadd and setex failed, err: %s", err)
	}

	return nil
}

var scriptZSetToStream = redis.NewScript(`
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES','LIMIT', 0, 1000);
if #members > 0 then
	  for key, value in ipairs(members)
	  do
		if key % 2 == 1 then
			redis.call('ZREM', KEYS[1], value);
			redis.call('XADD', KEYS[2], 'MAXLEN', '~', 1000, '*', 'dtime', members[key+1], 'uuid', value);
		end
	  end
  return #members;
else
  return 0;
end
`)

func (r *rdb) zSetToStream(ctx context.Context, zset, stream string, until time.Time) (cnt int, err error) {
	return scriptZSetToStream.Run(ctx, r, []string{zset, stream}, until.UnixMilli()).Int()
}

func (r *rdb) takeMessageStream(ctx context.Context, list, retry, data string, blockTimeout time.Duration, retryEnable bool, retryInterval time.Duration, expSec int) (id string, retryCount int, retdata map[string]interface{}, err error) {
	arr, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "dq",
		Consumer: "dq",
		Streams:  []string{list, ">"},
		Count:    1,
		Block:    blockTimeout,
		NoAck:    false,
	}).Result()

	if len(arr) > 0 && len(arr[0].Messages) > 0 {
		return arr[0].Messages[0].ID, 0, arr[0].Messages[0].Values, nil
	}

	return "", 0, nil, takeNil
}

func (r *rdb) takeMessageStreamRetry(ctx context.Context, list, retry, data string, retryEnable bool, retryInterval time.Duration, maxRetry int, expSec int, ackChan chan<- string) (id string, retryCount int, retdata map[string]interface{}, err error) {
	// TODO rename retried to delivered_times
	messages, _, err := r.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   list,
		Group:    "dq",
		MinIdle:  retryInterval,
		Start:    "-",
		Count:    1,
		Consumer: "dq",
	}).Result()
	if len(messages) == 0 {
		return "", 0, nil, takeNil
	}

	pendings, err := r.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   list,
		Group:    "dq",
		Start:    messages[0].ID,
		End:      messages[0].ID,
		Count:    1,
		Consumer: "dq",
	}).Result()
	if len(pendings) == 0 {
		return "", 0, nil, takeNil
	}

	if int(pendings[0].RetryCount) > maxRetry {
		// reach max retry count, ack it
		ackChan <- pendings[0].ID
		// TODO add to dead process
		return "", 0, nil, takeNil
	}

	return messages[0].ID, int(pendings[0].RetryCount) - 1, messages[0].Values, nil
}

// ready list will be removed by the consumer,
// so we only need to remove the message from the retry set and the data
var scriptCommit = redis.NewScript(`
local id = ARGV[1];
redis.call('ZREM', KEYS[1], id);
redis.call('ZREM', KEYS[2], id);
redis.call('DEL', KEYS[3] .. ':' .. id);
return 1;`)

func (r *rdb) commit(ctx context.Context, delay, retry, data, id string) (int64, error) {
	return scriptCommit.Run(ctx, r, []string{delay, retry, data}, id).Int64()
}
