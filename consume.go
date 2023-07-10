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
	msgCh := make(chan *ConsumerMessage, q.consumeWorkerNum)
	ackCh := make(chan string, q.consumeWorkerNum)

	ackCtx, ackCancel := context.WithCancel(ctx)
	q.ackCancel = ackCancel
	go func() {
		q.ackDone = make(chan error, 1)
		q.ackDone <- q.ack(ackCtx, ackCh)
	}()

	processCtx, processCancel := context.WithCancel(ctx)
	q.processCancel = processCancel
	go func() {
		q.processDone = make(chan error, 1)
		q.processDone <- q.process(processCtx, msgCh, ackCh, h)
	}()

	retryCtx, retryCancel := context.WithCancel(ctx)
	q.retryCancel = retryCancel
	go func() {
		q.retryDone = make(chan error, 1)
		q.retryDone <- q.retry(retryCtx, msgCh, ackCh)
	}()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	q.consumeCancel = consumeCancel
	go func() {
		q.consumeDone = make(chan error, 1)
		q.consumeDone <- q.consume(consumeCtx, msgCh)
	}()

	q.consumeStatus.Store(true)
}

func (q *Queue) ack(ctx context.Context, ackCh chan string) error {
	for {
		select {
		case <-ctx.Done():
			var ids []string
			for ; len(ackCh) > 0; ids = append(ids, <-ackCh) {
			}
			if len(ids) != 0 {
				cnt, err := q.rdb.XAck(ctx, q.key(kReady), q.streamGroup, ids...).Result()
				if err != nil || cnt != int64(len(ids)) {
					q.log(ctx, Warn, "commit message failed, want: %d, cnt: %d, err: %v", len(ids), cnt, err)
				}
			}

			q.log(ctx, Trace, "ack worker exited")
			return nil
		case id := <-ackCh:
			cnt, err := q.rdb.XAck(ctx, q.key(kReady), q.streamGroup, id).Result()
			if err != nil || cnt == 0 {
				q.log(ctx, Warn, "commit message failed, id: %s, cnt: %d, err: %v", id, cnt, err)
				ackCh <- id
				continue
			}
			q.log(ctx, Trace, "commit message success, id: %v", id)
		}
	}
}

func (q *Queue) consume(ctx context.Context, msgCh chan<- *ConsumerMessage) error {
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
				case <-ctx.Done():
					wg.Done()
					return
				case <-lastOk:
				case <-ticker.C:
					for ; len(lastOk) > 0; <-lastOk {
					}
				}

				cm, err := q.takeNormal(ctx)
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
	return nil
}

func (q *Queue) retry(ctx context.Context, msgCh chan<- *ConsumerMessage, ackChan chan<- string) error {
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

			start := ""
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case <-lastOk:
				case <-ticker.C:
					for ; len(lastOk) > 0; <-lastOk {
					}
				}

				cm, startNew, err := q.takeRetry(ctx, start, ackChan)
				start = startNew
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
	return nil
}

var takeNil = errors.New("take nil")

func (q *Queue) takeNormal(ctx context.Context) (*ConsumerMessage, error) {
	id, data, retryCount, err := q.rdb.streamRead(ctx,
		q.key(kReady),
		q.streamGroup,
		q.streamConsumer,
		q.consumeTakeBlockTimeout)

	if err == takeNil {
		return nil, takeNil
	}

	if err != nil {
		return nil, fmt.Errorf("take message failed, err: %v", err)
	}

	m, err := q.assemble(ctx, id, data, retryCount)
	if m == nil {
		return nil, fmt.Errorf("assemble message failed, data: %v, err: %v", data, err)
	}
	return m, nil
}

func (q *Queue) takeRetry(ctx context.Context, start string, ackChan chan<- string) (*ConsumerMessage, string, error) {
	if start == "" {
		start = "-"
	}
	id, retryCount, startNew, data, err := q.rdb.streamRetry(ctx,
		q.key(kReady),
		q.streamGroup,
		q.streamConsumer,
		q.retryInterval,
		start)

	if retryCount >= q.retryTimes {
		// reach max retry count, ack it
		ackChan <- id
		// TODO add to dead process
		return nil, "", takeNil
	}

	if err == takeNil {
		return nil, "", takeNil
	}

	if err != nil {
		return nil, "", fmt.Errorf("takeNormal message failed, err: %v", err)
	}

	m, err := q.assemble(ctx, id, data, retryCount)
	if m == nil {
		return nil, "", fmt.Errorf("assemble message failed, data: %v, err: %v", data, err)
	}

	return m, startNew, nil
}

func (q *Queue) process(ctx context.Context, msgCh <-chan *ConsumerMessage, ackCh chan<- string, h Handler) error {
	var wg sync.WaitGroup
	wg.Add(q.consumeWorkerNum)

	for i := 0; i < q.consumeWorkerNum; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case m := <-msgCh:
					func() {
						defer func() {
							if r := recover(); r != nil {
								q.log(ctx, Error, "process panic, err: %v", r)
							}
						}()

						handlerCtx, cancel := context.WithTimeout(ctx, q.consumeTimeout)
						defer cancel()
						id := m.ID
						if m.uuid != "" {
							m.ID = m.uuid
						}
						if err := h.Process(handlerCtx, m); err != nil {
							q.log(ctx, Warn, "process message failed, id: %s, err: %v", m.ID, err)
							return
						}
						q.log(ctx, Trace, "process message success, id: %s", m.ID)

						if !q.retryEnable {
							return
						}
						ackCh <- id
					}()
				}
			}
		}(i)
	}

	wg.Wait()
	q.log(ctx, Trace, "process workers exited")
	return nil
}

type msgData struct {
	Ctime string `json:"ctime"`
	Dtime string `json:"dtime"`
	Msg   string `json:"msg"`
}

func (q *Queue) assemble(ctx context.Context, id string, data map[string]interface{}, retryCount int) (*ConsumerMessage, error) {
	if uuid, ok := data["uuid"]; ok {
		// TODO need get data from redis
		raw, _ := q.rdb.Get(ctx, q.key(kMsg)+":"+uuid.(string)).Result()
		// TODO handle err
		// TODO if not exist, identify as canceled message, will not process
		var dataNew msgData
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
