package dq

import "time"

// Metric defines the interface for metrics
type Metric interface {
	Produce(isDelayMsg bool, err error)
	Consume(delay time.Duration, retried int, err error)
	Queue(ready, delay, retry int)
}
