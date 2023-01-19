package gossamer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Continuer decides whether to continue retry
type Continuer interface {
	Continue(ctx context.Context, param ContinueParam) bool
}

// ContinueParam helps you make a decision
type ContinueParam struct {
	Message kafka.Message
	Error   error
	Attempt int
}
