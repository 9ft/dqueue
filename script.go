package dq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// scriptProduceRealtimeMsg is used to produce realtime message
// 1. LPUSH list
// 2. HSET msg
// 3. EXPIRE msg
var scriptProduceRealtimeMsg = redis.NewScript(`
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], unpack(ARGV, 3, #ARGV))
redis.call('EXPIRE', KEYS[2], ARGV[2])`)

func (r *rdb) runProduceRealtimeMsg(ctx context.Context, list, data string, m *Message, expSec int) error {
	err := scriptProduceRealtimeMsg.Run(ctx, r,
		[]string{list, data + ":" + m.ID}, append([]interface{}{m.ID, expSec}, m.values()...)).Err()
	if err != redis.Nil && err != nil {
		return fmt.Errorf("script produce realtime msg failed, err: %s", err)
	}
	return nil
}

// scriptProduceDelayMsg is used to produce delay message
// 1. ZADD delay
// 2. HSET msg
// 3. EXPIRE msg
var scriptProduceDelayMsg = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[2], unpack(ARGV, 4, #ARGV))
redis.call('EXPIRE', KEYS[2], ARGV[3])`)

func (r *rdb) runProduceDelayMsg(ctx context.Context, zset, data string, m *Message, expSec int) error {
	err := scriptProduceDelayMsg.Run(ctx, r,
		[]string{zset, data + ":" + m.ID}, append([]interface{}{m.ID, m.DeliverAt.UnixMilli(), expSec}, m.values()...)).Err()
	if err != redis.Nil && err != nil {
		return fmt.Errorf("script produce delay msg failed, err: %s", err)
	}
	return nil
}

var scriptZsetToList = redis.NewScript(`
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

func (r *rdb) runZsetToList(ctx context.Context, zset, list string, until time.Time) (cnt int, err error) {
	return scriptZsetToList.Run(ctx, r, []string{zset, list}, until.UnixMilli()).Int()
}

// scriptTakeMessage is used to take message
// 1. RPOP list
// 2. EXIST msg
// 3. INCRBY msg
// 4. ZADD retry
// 5. HGETALL msg
var scriptTakeMsg = redis.NewScript(
	fmt.Sprintf(`
local id = redis.call('RPOP', KEYS[1]);
if id == false then
	return {'%s'};
end

local exist = redis.call('EXISTS', KEYS[3] .. ':' .. id);
if exist == 0 then
	return {'%s'};
end

local cnt = redis.call('HINCRBY', KEYS[3] .. ':' .. id, 'deliver_cnt', 1);
if cnt-1 > tonumber(ARGV[2]) then
	redis.call('DEL', KEYS[3] .. ':' .. id);
	return {'%s'};
end

redis.call('ZADD', KEYS[2], ARGV[1], id);
return redis.call('HGETALL', KEYS[3] .. ':' .. id);`,
		listEmpty.Error(),
		dataMiss.Error(),
		deliverCntExceed.Error()))

var (
	listEmpty        = errors.New("list empty")
	dataMiss         = errors.New("data miss")
	deliverCntExceed = errors.New("deliver cnt exceed")
)

func (r *rdb) runTakeMsg(ctx context.Context, list, retry, data string, retryInterval time.Duration, retryTimes int) ([]string, error) {
	retryAt := time.Now().Add(retryInterval)
	s, err := scriptTakeMsg.Run(ctx, r, []string{list, retry, data}, retryAt.UnixMilli(), retryTimes).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("script run failed, err: %v", err)
	}
	if len(s) == 1 {
		switch s[0] {
		case listEmpty.Error():
			return nil, listEmpty
		case dataMiss.Error():
			return nil, dataMiss
		case deliverCntExceed.Error():
			return nil, deliverCntExceed
		default:
			return nil, fmt.Errorf("script run failed, s[0]: %v", s[0])
		}
	}
	return s, nil
}

// ready list will be removed by the consumer,
// so we only need to remove the message from the retry set and the data
var scriptCommit = redis.NewScript(`
local id = ARGV[1];
redis.call('ZREM', KEYS[1], id);
redis.call('DEL', KEYS[2] .. ':' .. id);
return 1;`)

func (r *rdb) runCommit(ctx context.Context, retry, data, id string) (int64, error) {
	return scriptCommit.Run(ctx, r, []string{retry, data}, id).Int64()
}

var scriptZaddAndHset = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]);
local exist = redis.call('EXISTS', KEYS[2] .. ':' .. ARGV[2]);
if exist == 0 then
	return nil;
end
return redis.call('HSET', KEYS[2] .. ':' .. ARGV[2], 're_deliver_at', ARGV[1]);
`)

func (r *rdb) runZaddAndHset(ctx context.Context, retry, data, id string, at time.Time) error {
	return scriptZaddAndHset.Run(ctx, r, []string{retry, data}, at.UnixMilli(), id).Err()
}
