package dq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type ConsumerMessage struct {
	ID        string
	uuid      string
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
func (q *Queue) Consume(ctx context.Context, h HandlerFunc) {
	q.shutdown = make(chan struct{})
	q.consumeDone = make(chan struct{})
	q.retryDone = make(chan struct{})
	q.processDone = make(chan struct{})
	q.ackDone = make(chan struct{})
	q.done = make(chan struct{})

	msgCh := make(chan *ConsumerMessage, q.consumeWorkerNum)
	ackCh := make(chan string, q.consumeWorkerNum)

	go q.ack(ctx, ackCh)
	go q.process(ctx, msgCh, ackCh, h)
	go q.retry(ctx, msgCh, ackCh)
	go q.consume(ctx, msgCh)
}

func (q *Queue) ack(ctx context.Context, ackCh chan string) {
	for {
		select {
		case <-q.ackDone:

			var ids []string
			for len(ackCh) > 0 {
				ids = append(ids, <-ackCh)
			}
			if len(ids) != 0 {
				cnt, err := q.rdb.XAck(ctx, q.getKey(kReady), "dq", ids...).Result()
				if err != nil || cnt != int64(len(ids)) {
					q.log(ctx, Warn, "commit message failed, want: %d, cnt: %d, err: %v", len(ids), cnt, err)
				}
			}

			q.log(ctx, Trace, "ack worker exited")

			close(q.done)
			return
		case id := <-ackCh:
			cnt, err := q.rdb.XAck(ctx, q.getKey(kReady), "dq", id).Result()
			if err != nil || cnt == 0 {
				q.log(ctx, Warn, "commit message failed, id: %s, cnt: %d, err: %v", id, cnt, err)
				ackCh <- id
				continue
			}
			q.log(ctx, Trace, "commit message success, id: %v", id)
		}
	}
}

func (q *Queue) consume(ctx context.Context, msgCh chan<- *ConsumerMessage) {
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

				cm, err := q.takeMessage(ctx, q.consumeTakeBlockTimeout)
				if err == takeNil {
					continue
				}
				if err != nil {
					q.log(ctx, Warn, "consume message failed, err: %v", err)
					continue
				}

				msgCh <- cm
				lastOk <- struct{}{}
			}
		}(i)
	}

	wg.Wait()
	q.log(ctx, Trace, "consume workers exited")
	close(q.retryDone)
}

func (q *Queue) retry(ctx context.Context, msgCh chan<- *ConsumerMessage, ackChan chan<- string) {
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
				case <-q.retryDone:
					wg.Done()
					return
				case <-lastOk:
				case <-ticker.C:
					for ; len(lastOk) > 0; <-lastOk {
					}
				}

				cm, err := q.takeMessageRetry(ctx, ackChan)
				if err == takeNil {
					continue
				}
				if err != nil {
					q.log(ctx, Warn, "retry message failed, err: %v", err)
					continue
				}

				msgCh <- cm
				lastOk <- struct{}{}
			}
		}(i)
	}

	wg.Wait()
	q.log(ctx, Trace, "retry workers exited")
	close(q.processDone)
}

var takeNil = errors.New("take nil")

func (q *Queue) takeMessage(ctx context.Context, blockTimeout time.Duration) (*ConsumerMessage, error) {
	rq := q.getKey(kReady) // list
	pq := q.getKey(kRetry) // zset
	mq := q.getKey(kMsg)

	id, retryCount, data, err := q.rdb.takeMessageStream(ctx,
		rq, pq, mq,
		blockTimeout,
		q.retryEnable,
		q.retryInterval,
		int(q.messageSaveTime.Seconds()))

	if err == takeNil {
		return nil, takeNil
	}

	if err != nil {
		return nil, fmt.Errorf("take message failed, err: %v", err)
	}

	cm, err := parseData(ctx, q.rdb, mq, id, retryCount, data)
	if cm == nil {
		return nil, fmt.Errorf("parse message failed, data: %v, err: %v", data, err)
	}
	return cm, nil
}

func (q *Queue) takeMessageRetry(ctx context.Context, ackChan chan<- string) (*ConsumerMessage, error) {
	rq := q.getKey(kReady) // list
	pq := q.getKey(kRetry) // zset
	mq := q.getKey(kMsg)

	id, retryCount, data, err := q.rdb.takeMessageStreamRetry(ctx,
		rq, pq, mq,
		q.retryEnable,
		q.retryInterval,
		q.retryTimes,
		int(q.messageSaveTime.Seconds()),
		ackChan)

	if err == takeNil {
		return nil, takeNil
	}

	if err != nil {
		return nil, fmt.Errorf("take message failed, err: %v", err)
	}

	cm, err := parseData(ctx, q.rdb, mq, id, retryCount, data)
	if cm == nil {
		return nil, fmt.Errorf("parse message failed, data: %v, err: %v", data, err)
	}
	return cm, nil
}

func (q *Queue) process(ctx context.Context, msgCh <-chan *ConsumerMessage, ackCh chan<- string, h Handler) {
	defer func() {
		if r := recover(); r != nil {
			q.log(ctx, Error, "process panic, err: %v", r)
		}
	}()

	for i := len(q.mws) - 1; i >= 0; i-- {
		h = q.mws[i](h)
	}

	var wg sync.WaitGroup
	wg.Add(q.consumeWorkerNum)

	for i := 0; i < q.consumeWorkerNum; i++ {
		go func(i int) {
			for {
				select {
				case <-q.processDone:
					wg.Done()
					return
				case m := <-msgCh:
					handlerCtx, cancel := context.WithTimeout(ctx, q.consumeTimeout)
					defer cancel()
					id := m.ID
					if m.uuid != "" {
						m.ID = m.uuid
					}
					if err := h.Process(handlerCtx, m); err != nil {
						q.log(ctx, Warn, "process message failed, id: %s, err: %v", m.ID, err)
						continue
					}
					q.log(ctx, Trace, "process message success, id: %s", m.ID)

					if !q.retryEnable {
						continue
					}

					ackCh <- id
				}
			}
		}(i)
	}

	wg.Wait()

	q.log(ctx, Trace, "process workers exited")
	close(q.ackDone)
}

type messageBody struct {
	ID    string `json:"id"`
	DTime int    `json:"dtime"`
	Data  string `json:"msg"`
	CTime int64  `json:"ctime"`
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

type tmpdata struct {
	Ctime string `json:"ctime"`
	Dtime string `json:"dtime"`
	Msg   string `json:"msg"`
}

func parseData(ctx context.Context, r rdb, mq string, id string, retryCount int, data map[string]interface{}) (*ConsumerMessage, error) {
	if uuid, ok := data["uuid"]; ok {
		// TODO need get data from redis
		raw, _ := r.Get(ctx, mq+":"+uuid.(string)).Result()
		// TODO handle err
		// TODO if not exist, identify as canceled message, will not process
		var dataNew tmpdata
		if err := json.Unmarshal([]byte(raw), &dataNew); err != nil {
			return nil, fmt.Errorf("json unmarshal failed, err: %v", err)
		}

		payload, _ := base64.StdEncoding.DecodeString(dataNew.Msg)
		ctime, _ := strconv.ParseInt(dataNew.Ctime, 10, 64)
		dtime, _ := strconv.ParseInt(dataNew.Dtime, 10, 64)
		dd := time.UnixMilli(dtime)

		cm := &ConsumerMessage{
			ID:        id,
			uuid:      uuid.(string),
			Payload:   payload,
			Retried:   int32(retryCount),
			CreateAt:  time.UnixMilli(ctime),
			DeliverAt: &dd,
		}

		return cm, nil
	}

	// TODO handle err
	payload, _ := base64.StdEncoding.DecodeString(data["msg"].(string))
	ctimeStr := data["ctime"].(string)
	ctime, _ := strconv.ParseInt(ctimeStr, 10, 64)

	cm := &ConsumerMessage{
		ID:       id,
		Payload:  payload,
		Retried:  int32(retryCount),
		CreateAt: time.UnixMilli(ctime),
		// DeliverAt: nil,
	}

	return cm, nil
}
