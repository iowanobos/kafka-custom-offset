package gossamer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	broker = "localhost:9092"
	topic  = "test_topic"
)

func TestGroup(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	delays := []time.Duration{10 * time.Second, 9 * time.Second, 13 * time.Second}

	conn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err)

	expectedDelayChan := make(chan time.Duration, len(delays))
	retryTopics := make([]RetryTopicOption, len(delays))
	for i, delay := range delays {
		expectedDelayChan <- delay

		retryTopic := topic + "_retry_" + delay.String()
		retryTopics[i] = RetryTopicOption{
			Delay: delay,
			Topic: retryTopic,
		}

		if err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             retryTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			require.NoError(t, err)
		}
	}

	consumeResultChan := make(chan error)
	exit := make(chan struct{})
	go func() {
		for range delays {
			consumeResultChan <- errors.New("some error")
		}
		consumeResultChan <- nil
		close(exit)
	}()

	group, err := NewGroup(
		Options{
			Brokers:              []string{broker},
			Group:                "test_" + uuid.New().String(),
			Topic:                topic,
			RetryTopics:          retryTopics,
			PartitionWorkerCount: 1,
		})
	require.NoError(t, err)

	go sendMessage(t)

	go func() {
		var prevConsumeTime *time.Time

		group.Run(ConsumerFunc(func(ctx context.Context, message kafka.Message) error {
			now := time.Now()

			if prevConsumeTime != nil {
				actualDelay := now.Sub(*prevConsumeTime)
				t.Log("Delay: ", actualDelay.String())

				assert.GreaterOrEqual(t, actualDelay, <-expectedDelayChan, "delay is incorrect")
			}
			prevConsumeTime = &now

			return <-consumeResultChan
		}))
	}()

	select {
	case <-time.After(time.Minute):
		t.Error("timeout")
	case <-exit:
		group.Close()
	}
}

func sendMessage(t *testing.T) {
	t.Helper()

	w := &kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: topic,
	}

	value, _ := uuid.New().MarshalBinary()
	if err := w.WriteMessages(context.Background(), kafka.Message{Value: value}); err != nil {
		return
	}
}
