package dq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ConsumerMessage struct {
	ID        string
	Payload   []byte
	Retried   int32
	CreateAt  time.Time
	DeliverAt *time.Time
}

type Handler interface {
	Process(context.Context, *ConsumerMessage) error
}

type HandlerFunc func(context.Context, *ConsumerMessage) error

func (h HandlerFunc) Process(ctx context.Context, message *ConsumerMessage) error {
	return h(ctx, message)
}

type middlewareFunc func(Handler) Handler

// Consume use Handler to process message
func (q *Queue) Consume(ctx context.Context, h Handler) {
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
			defer func() {
				if r := recover(); r != nil {
					q.log(ctx, Error, "consume panic, worker: %d, err: %v", i, r)
				}
			}()

			ticker := time.NewTicker(q.consumeWorkerInterval)
			defer ticker.Stop()

			lastOk := make(chan struct{}, 1)
			for {
				select {
				case <-q.shutdown:
					wg.Done()
					return
				case <-lastOk:
				case <-ticker.C:
					for ; len(lastOk) > 0; <-lastOk {
					}
				}

				err := q.process(ctx, h)
				if err == takeNil {
					continue
				}
				if err != nil {
					q.log(ctx, Warn, "process message failed, err: %v", err)
					continue
				}
				lastOk <- struct{}{}
			}
		}(i)
	}

	wg.Wait()
	q.log(ctx, Trace, "all consume worker exited, num: %d", q.consumeWorkerNum)
	q.done <- struct{}{}
}

var takeNil = errors.New("take nil")

func (q *Queue) process(ctx context.Context, h Handler) error {
	rq := q.getKey(kReady) // list
	pq := q.getKey(kRetry) // zset
	mq := q.getKey(kMsg)

	text, err := q.rdb.takeMessage(ctx, rq, pq, mq, q.retryEnable, q.retryInterval, int(q.messageSaveTime.Seconds()))
	if err != nil {
		return fmt.Errorf("take message failed, err: %v", err)
	}

	if text == "" {
		return takeNil
	}

	cm, err := parse(text)
	if cm == nil {
		return fmt.Errorf("parse message failed, text: %s, err: %v", text, err)
	}

	err = h.Process(ctx, cm)
	if err != nil {
		return fmt.Errorf("process message failed, err: %v", err)
	}

	if !q.retryEnable {
		return nil
	}

	_, err = q.rdb.commit(ctx, q.getKey(kDelay), q.getKey(kRetry), q.getKey(kMsg), cm.ID)
	if err != nil {
		return fmt.Errorf("commit message failed, err: %v", err)
	}

	return nil
}

type messageBody struct {
	ID    string `json:"id"`
	DTime int    `json:"dtime"`
	Data  string `json:"data"`
	CTime int    `json:"ctime"`
	Retry int    `json:"retry"`
}

func parse(text string) (*ConsumerMessage, error) {
	var body messageBody
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		return nil, fmt.Errorf("json unmarshal failed, err: %v", err)
	}

	bs, err := base64.StdEncoding.DecodeString(body.Data)
	if err != nil {
		return nil, fmt.Errorf("base64 decode failed, err: %v", err)
	}

	return &ConsumerMessage{
		ID:       body.ID,
		Payload:  bs,
		CreateAt: time.UnixMilli(int64(body.CTime)),
		DeliverAt: func() *time.Time {
			if body.DTime == 0 {
				return nil
			}
			t := time.UnixMilli(int64(body.DTime))
			return &t
		}(),
		Retried: int32(body.Retry),
	}, nil
}
