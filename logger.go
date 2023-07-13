package dq

import (
	"context"
	"log"
)

type LogLevel int

const (
	Silent LogLevel = iota
	Error
	Warn
	Info
	Trace
	numLevel = 5
)

type Logger interface {
	Error(context.Context, string, ...interface{})
	Warn(context.Context, string, ...interface{})
	Info(context.Context, string, ...interface{})
	Trace(context.Context, string, ...interface{})
}

func (q *Queue) log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {
	if q.opts.logger == nil || level > q.logMode {
		return
	}

	l := q.opts.logger
	switch level {
	case Error:
		l.Error(ctx, msg, data...)
	case Warn:
		l.Warn(ctx, msg, data...)
	case Info:
		l.Info(ctx, msg, data...)
	case Trace:
		l.Trace(ctx, msg, data...)
	}
}

type defaultLogger struct{}

func (l defaultLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("ERROR: "+msg, data...)
}

func (l defaultLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("WARN: "+msg, data...)
}

func (l defaultLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("INFO: "+msg, data...)
}

func (l defaultLogger) Trace(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("TRACE: "+msg, data...)
}
