package dq

import (
	"context"
	"sync"
	"time"
)

func (q *Queue) daemon(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(q.daemonWorkerNum)

	for i := 0; i < q.daemonWorkerNum; i++ {
		go func(i int) {
			ticker := time.NewTicker(q.daemonWorkerInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case <-ticker.C:
				}

				go func() {
					ctx := context.Background()
					cnt, err := q.rdb.runZsetToList(ctx, q.key(kDelay), q.key(kReady), time.Now())
					if err != nil {
						q.log(ctx, Warn, "daemon, delay to ready failed, err: %v", err)
						return
					}
					if cnt > 0 {
						q.log(ctx, Trace, "daemon, delay to ready, cnt: %d", cnt)
					}
				}()

				go func() {
					ctx := context.Background()
					cnt, err := q.rdb.runZsetToList(ctx, q.key(kRetry), q.key(kReady), time.Now())
					if err != nil {
						q.log(ctx, Warn, "daemon, retry to ready failed, err: %v", err)
						return
					}
					if cnt > 0 {
						q.log(ctx, Trace, "daemon, retry to ready, cnt: %d", cnt)
					}
				}()

				go func() {
					ctx := context.Background()
					if q.opts.metric != nil {
						go q.opts.metric.Queue(
							int(q.rdb.LLen(ctx, q.key(kReady)).Val()),
							int(q.rdb.ZCard(ctx, q.key(kDelay)).Val()),
							int(q.rdb.ZCard(ctx, q.key(kRetry)).Val()))
					}
				}()
			}
		}(i)
	}

	wg.Wait()
	q.log(context.Background(), Trace, "all daemon worker exited")
}
