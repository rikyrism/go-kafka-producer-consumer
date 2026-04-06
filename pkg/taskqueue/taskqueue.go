package taskqueue

import "context"

// TaskQueue is a generic interface for a task queue.
// T is the payload type to be sent/received.
type TaskQueue[T any] interface {
	// Publish sends the payload to the queue
	Publish(ctx context.Context, topic string, payload T) error

	// Start starts the consumer and calls the handler whenever a message comes in
	Start(ctx context.Context, topic string, handler func(payload T) error) error

	// Close closes the connection
	Close() error
}
