package dq

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type opts struct {
	// basic
	name string

	// daemon
	daemonWorkerNum      int
	daemonWorkerInterval time.Duration

	// consumer
	consumeWorkerNum      int
	consumeWorkerInterval time.Duration
	consumeTimeout        time.Duration
	retryTimes            int
	retryInterval         time.Duration

	// middleware
	mws []middlewareFunc

	// message
	messageSaveTime time.Duration

	// logger
	logMode LogLevel
	logger  Logger
}

func defaultOpts() opts {
	return opts{
		name: "default",

		daemonWorkerNum:      1,
		daemonWorkerInterval: 100 * time.Millisecond,

		consumeWorkerNum:      2,
		consumeWorkerInterval: 100 * time.Millisecond,
		consumeTimeout:        3 * time.Second,
		retryTimes:            3,
		retryInterval:         3 * time.Second,

		mws: nil,

		messageSaveTime: 30 * 24 * time.Hour,

		logMode: Silent,
		logger:  defaultLogger{},
	}
}

func WithName(name string) func(*Queue) {
	return func(q *Queue) {
		q.name = name
	}
}

func WithDaemonWorkerNum(num int) func(*Queue) {
	return func(q *Queue) {
		q.daemonWorkerNum = num
	}
}

func WithDaemonWorkerInterval(interval time.Duration) func(*Queue) {
	return func(q *Queue) {
		q.daemonWorkerInterval = interval
	}
}

func WithConsumerWorkerNum(num int) func(*Queue) {
	return func(q *Queue) {
		q.consumeWorkerNum = num
	}
}

func WithConsumerTimeout(timeout time.Duration) func(*Queue) {
	return func(q *Queue) {
		q.consumeTimeout = timeout
	}
}

func WithConsumerWorkerInterval(interval time.Duration) func(*Queue) {
	return func(q *Queue) {
		q.consumeWorkerInterval = interval
	}
}

func WithRetryTimes(times int) func(*Queue) {
	return func(q *Queue) {
		q.retryTimes = times
	}
}

func WithRetryInterval(interval time.Duration) func(*Queue) {
	return func(q *Queue) {
		q.retryInterval = interval
	}
}

func WithMiddleware(mws ...middlewareFunc) func(*Queue) {
	return func(q *Queue) {
		q.mws = mws
	}
}

func WithMessageSaveTime(saveTime time.Duration) func(*Queue) {
	return func(q *Queue) {
		q.messageSaveTime = saveTime
	}
}

func WithLogMode(mode LogLevel) func(*Queue) {
	return func(q *Queue) {
		q.logMode = mode
	}
}

func WithLogger(logger Logger) func(*Queue) {
	return func(q *Queue) {
		q.logger = logger
	}
}

func WithRedis(rdb *redis.Client) func(*Queue) {
	return func(q *Queue) {
		q.rdb.Client = rdb
	}
}

func WithRedisKeyPrefix(prefix string) func(*Queue) {
	return func(q *Queue) {
		q.rdb.redisPrefix = prefix
	}
}
