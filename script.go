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
  return #members / 2;
else
  return 0;
end
`)

func (r *rdb) zSetToStream(ctx context.Context, zset, stream string, until time.Time) (cnt int, err error) {
	return scriptZSetToStream.Run(ctx, r, []string{zset, stream}, until.UnixMilli()).Int()
}

func (r *rdb) streamRead(ctx context.Context, stream, group, consumer string, blockTimeout time.Duration) (
	id string, data map[string]interface{}, retryCount int, err error) {

	arr, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    blockTimeout,
		NoAck:    false,
	}).Result()

	if len(arr) > 0 && len(arr[0].Messages) > 0 {
		return arr[0].Messages[0].ID, arr[0].Messages[0].Values, 0, nil
	}

	return "", nil, 0, takeNil
}

// TODO move ackChan
// TODO line too long
func (r *rdb) streamRetry(ctx context.Context, stream, group, consumer string, retryInterval time.Duration, start string) (
	id string, retryCount int, startNew string, data map[string]interface{}, err error) {

	// TODO rename retried to delivered_times
	messages, startNew, err := r.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:  stream,
		Group:   group,
		MinIdle: retryInterval,
		// Start:    "-",
		Start:    start,
		Count:    1,
		Consumer: consumer,
	}).Result()
	if len(messages) == 0 {
		return "", 0, startNew, nil, takeNil
	}

	pendings, err := r.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   stream,
		Group:    group,
		Start:    messages[0].ID,
		End:      messages[0].ID,
		Count:    1,
		Consumer: consumer,
	}).Result()
	if len(pendings) == 0 {
		return "", 0, startNew, nil, takeNil
	}

	return messages[0].ID, int(pendings[0].RetryCount) - 1, startNew, messages[0].Values, nil
}

// ready list will be removed by the consumer,
// so we only need to remove the message from data
var scriptCommit = redis.NewScript(`
local id = ARGV[1];
redis.call('ZREM', KEYS[1], id);
redis.call('DEL', KEYS[2] .. ':' .. id);
return 1;`)

func (r *rdb) commit(ctx context.Context, delay, data, id string) (int64, error) {
	return scriptCommit.Run(ctx, r, []string{delay, data}, id).Int64()
}
