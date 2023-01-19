package gossamer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer consumes messages
type Consumer interface {
	Consume(ctx context.Context, message kafka.Message) error
}

// ConsumerFunc allows consuming messages without implementation Consumer
type ConsumerFunc func(ctx context.Context, message kafka.Message) error

func (f ConsumerFunc) Consume(ctx context.Context, message kafka.Message) error {
	return f(ctx, message)
}
