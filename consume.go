package dq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Handler interface {
	Process(context.Context, *Message) error
}

type HandlerFunc func(context.Context, *Message) error

func (h HandlerFunc) Process(ctx context.Context, m *Message) error {
	return h(ctx, m)
}

type middlewareFunc func(Handler) Handler

// Consume use Handler to process message
func (q *Queue) Consume(h Handler) {
	ctx, cancel := context.WithCancel(context.Background())
	q.shutdownFunc = cancel

	q.done = make(chan struct{})

	go q.daemon(ctx)
	go q.consume(ctx, h)
}

func (q *Queue) consume(ctx context.Context, h Handler) {
	for i := len(q.mws) - 1; i >= 0; i-- {
		h = q.mws[i](h)
	}

	var wg sync.WaitGroup
	wg.Add(q.consumeWorkerNum)

	for i := 0; i < q.consumeWorkerNum; i++ {
		go func(i int) {
			switch {
			case q.lim != nil:
				// Limiter Mode consume message with limiter, consume next message after limiter wait.
				q.consumeWithLimiter(ctx, h)
				wg.Done()
			default:
				// Ticker Mode consume message with ticker,
				// 	if success, consume next message immediately.
				// 	if failed, consume next message after consumeWorkerInterval.
				q.consumeWithTicker(ctx, h)
				wg.Done()
			}
		}(i)
	}

	wg.Wait()
	q.log(context.Background(), Trace, "all consume worker exited")
	q.done <- struct{}{}
}

func (q *Queue) consumeWithTicker(ctx context.Context, h Handler) {
	ticker := time.NewTicker(q.consumeWorkerInterval)
	defer ticker.Stop()

	immed := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-immed:
		case <-ticker.C:
			for ; len(immed) > 0; <-immed {
			}
		}

		err := q.process(h)
		if errors.Is(err, skip) {
			immed <- struct{}{}
			continue
		}
		if errors.Is(err, wait) {
			continue
		}
		if err != nil {
			q.log(context.Background(), Warn, "process message failed, err: %v", err)
			continue
		}

		immed <- struct{}{}
	}
}

func (q *Queue) consumeWithLimiter(ctx context.Context, h Handler) {
	immed := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-immed:
		default:
			if err := q.lim.Wait(ctx); err != nil {
				q.log(ctx, Warn, "limiter wait failed, err: %v", err)
				continue
			}
			for ; len(immed) > 0; <-immed {
			}
		}

		err := q.process(h)
		if errors.Is(err, skip) {
			immed <- struct{}{}
			continue
		}
		if errors.Is(err, wait) {
			continue
		}
		if err != nil {
			q.log(context.Background(), Warn, "process message failed, err: %v", err)
		}
	}
}

var (
	skip = errors.New("skip")
	wait = errors.New("wait")
)

func (q *Queue) process(h Handler) error {
	rq := q.key(kReady) // list
	pq := q.key(kRetry) // zset
	mq := q.key(kData)

	ctx := context.Background()
	s, err := q.rdb.runTakeMsg(ctx, rq, pq, mq, q.retryInterval, q.retryTimes)

	if err != nil {
		switch {
		case errors.Is(err, dataMiss),
			errors.Is(err, deliverCntExceed):
			return skip
		case errors.Is(err, listEmpty):
			return wait
		default:
			return fmt.Errorf("take message failed, err: %v", err)
		}
	}

	var m Message
	if err = m.parse(s); err != nil {
		return fmt.Errorf("parse message failed, err: %v", err)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("process message panic: %v", r)
			}
		}()

		ctx, c := context.WithTimeout(ctx, q.consumeTimeout)
		defer c()
		err = h.Process(ctx, &m)
		if q.opts.metric != nil {
			start := time.Now()
			delay := start.Sub(m.CreateAt)
			if m.DeliverAt != nil {
				delay = start.Sub(*m.DeliverAt)
			}
			if m.ReDeliverAt != nil {
				delay = start.Sub(*m.ReDeliverAt)
			}
			go q.opts.metric.Consume(delay, m.DeliverCnt, err)
		}
		if err != nil {
			err = fmt.Errorf("process message failed, err: %v", err)
		}
	}()

	// if err occurs, not commit message
	if err != nil {
		return nil
	}

	_, err = q.rdb.runCommit(ctx, q.key(kRetry), q.key(kData), m.ID)
	if err != nil {
		return fmt.Errorf("commit message failed, err: %v", err)
	}

	return nil
}

func (q *Queue) RedeliveryAfter(ctx context.Context, id string, dur time.Duration) error {
	return q.RedeliveryAt(ctx, id, time.Now().Add(dur))
}

func (q *Queue) RedeliveryAt(ctx context.Context, id string, at time.Time) error {
	return q.rdb.runZaddAndHset(ctx, q.key(kRetry), q.key(kData), id, at)
}
