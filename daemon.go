package dq

import (
	"context"
	"time"
)

func (q *Queue) daemon() {
	for i := 0; i < q.daemonWorkerNum; i++ {
		go func() {
			ticker := time.NewTicker(q.daemonWorkerInterval)
			for {
				go func() {
					ctx := context.Background()
					cnt, err := q.rdb.zSetToStream(ctx, q.getKey(kDelay), q.getKey(kReady), time.Now())
					if err != nil {
						q.log(ctx, Warn, "daemon, delay to ready failed, err: %v", err)
						return
					}
					q.log(ctx, Trace, "daemon, delay to ready, cnt: %d", cnt)
				}()
				<-ticker.C
			}
		}()
	}
}
