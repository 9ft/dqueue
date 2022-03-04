//go:generate protoc --go_out=. --go_opt=paths=source_relative entity.proto

package dqueue

import "time"

type ProducerMessage struct {
	Payload   []byte
	Value     interface{}
	DeliverAt *time.Time
}
