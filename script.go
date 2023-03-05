package dq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var scriptLPushSetEx = redis.NewScript(`
redis.call('LPUSH', KEYS[1], ARGV[1])

local ctime = tonumber(ARGV[3])
local dtime = tonumber(ARGV[4])

local msg = cjson.encode({id=ARGV[1], ctime=ctime, dtime=dtime, data=ARGV[2]})
redis.call('SET', KEYS[2] .. ':' .. ARGV[1], msg, 'EX', ARGV[5])`)

func (r *rdb) lPushSetEx(ctx context.Context, list, data, id, msg string, createAt time.Time, deliverAt time.Time, expSec int) error {
	err := scriptLPushSetEx.Run(ctx, r, []string{list, data}, id, msg, createAt.UnixMilli(), deliverAt.UnixMilli(), expSec).Err()
	if err != redis.Nil && err != nil {
		return fmt.Errorf("lpush and setex failed, err: %s", err)
	}
	return nil
}

var scriptZAddSetEx = redis.NewScript(`
local dtime = tonumber(ARGV[4])
local ctime = tonumber(ARGV[3])

redis.call('ZADD', KEYS[1], dtime, ARGV[1])
local msg = cjson.encode({id=ARGV[1],ctime=ctime, dtime=dtime, data=ARGV[2]})
redis.call('SET', KEYS[2] .. ':' .. ARGV[1], msg, 'EX', ARGV[5])`)

func (r *rdb) zAddSetEx(ctx context.Context, zset, data, id, msg string, createAt time.Time, deliverAt *time.Time, expSec int) error {
	err := scriptZAddSetEx.Run(ctx, r, []string{zset, data}, id, msg, createAt.UnixMilli(), deliverAt.UnixMilli(), expSec).Err()
	if err != redis.Nil && err != nil {
		return fmt.Errorf("zadd and setex failed, err: %s", err)
	}

	return nil
}

var scriptZSetToList = redis.NewScript(`
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1000);
if #members > 0 then
	  for key, value in ipairs(members)
  do
	redis.call('ZREM', KEYS[1], value);
	redis.call('LPUSH', KEYS[2], value);
  end
  return #members;
else
  return 0;
end`)

func (r *rdb) zSetToList(ctx context.Context, zset, list string, until time.Time) (cnt int, err error) {
	return scriptZSetToList.Run(ctx, r, []string{zset, list}, until.UnixMilli()).Int()
}

var scriptTakeMessage = redis.NewScript(`
local id = redis.call('RPOP', KEYS[1]);
if id == false then
	return nil;
end

local msg = redis.call('GET', KEYS[3] .. ':' .. id);
if msg == false then
	return nil;
end

if ARGV[1] == false then
	redis.call('DEL', KEYS[3] .. ':' .. id);
	return msg;
end

redis.call('ZADD', KEYS[2], ARGV[2], id);

local retried = 0;
local obj = cjson.decode(msg);
if obj.retry ~= nil then
	retried = obj.retry;
end
obj.retry = retried + 1;

local msg_new = cjson.encode(obj);
redis.call('SET', KEYS[3] .. ':' .. id, msg_new, 'EX', ARGV[3]);

return msg;`)

func (r *rdb) takeMessage(ctx context.Context, list, retry, data string, retryEnable bool, retryInterval time.Duration, expSec int) (text string, err error) {
	retryAt := time.Now().Add(retryInterval)
	s, err := scriptTakeMessage.Run(ctx, r, []string{list, retry, data}, retryEnable, retryAt.UnixMilli(), expSec).Text()
	if err != nil && err != redis.Nil {
		return "", fmt.Errorf("script run failed, err: %v", err)
	}
	return s, nil
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
