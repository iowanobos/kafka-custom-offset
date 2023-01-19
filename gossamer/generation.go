package gossamer

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/iowanobos/kafka-custom-offset/collections/queue"
)

type generation struct {
	config

	// core
	gen      *kafka.Generation
	producer *kafka.Writer

	// custom offsets
	processedRecords          map[string]map[int]*partitionProcessedOffsets
	processedMessageChan      chan kafka.Message
	partitionRegistrationChan chan kafka.Message

	// graceful shutdown
	workersWaitGroup  sync.WaitGroup
	commitLoopStopped chan struct{}
}

func (g *Group) newGeneration() (*generation, error) {
	gen, err := g.group.Next(g.gracefulShutdownCtx)
	if err != nil {
		return nil, err
	}

	producer := &kafka.Writer{
		Addr:         kafka.TCP(g.brokers...),
		Balancer:     &kafka.RoundRobin{},
		Async:        true,
		RequiredAcks: kafka.RequireAll,
	}

	if g.config.TLS != nil {
		tlsConfig, err := getTlsConfig(g.TLS)
		if err != nil {
			return nil, err
		}

		producer.Transport = &kafka.Transport{
			ClientID: g.config.TLS.ClientID,
			TLS:      tlsConfig,
		}
	}

	return &generation{
		config: g.config,

		gen:      gen,
		producer: producer,

		processedRecords:          make(map[string]map[int]*partitionProcessedOffsets, len(g.nextRetryTopicMap)),
		processedMessageChan:      make(chan kafka.Message),
		partitionRegistrationChan: make(chan kafka.Message),

		commitLoopStopped: make(chan struct{}),
	}, nil
}

func (g *generation) Run(ctx context.Context, consumer Consumer) {
	ctx, cancel := context.WithCancel(ctx)

	for topic, partitions := range g.gen.Assignments {
		g.processedRecords[topic] = make(map[int]*partitionProcessedOffsets)

		for _, partition := range partitions {
			messageChan := g.getMessageChan(topic, partition.ID, partition.Offset)

			g.workersWaitGroup.Add(g.workerCount)
			for i := 0; i < g.workerCount; i++ {
				go func() {
					defer g.workersWaitGroup.Done()

					for message := range messageChan {
						m := HeadersToMap(message.Headers)

						if retryAt, hasRetryAt := m.GetRetryAt(); hasRetryAt {
							select {
							case <-ctx.Done(): // graceful shutdown
								return
							case <-time.After(time.Until(retryAt)): // wait
							}
						}

						if err := consumer.Consume(ctx, message); err != nil {
							isRetrySucceeded := g.retry(ctx, consumer, m, message, err)
							if !isRetrySucceeded {
								continue
							}
						}

						select {
						case <-ctx.Done(): // graceful shutdown
							return
						case g.processedMessageChan <- message:
						}
					}
				}()
			}
		}
	}
	go func() {
		g.workersWaitGroup.Wait()
		cancel()
	}()

	go g.runCommitLoop(ctx)
	<-g.commitLoopStopped
}

// getMessageChan provides channel of fetched messages
func (g *generation) getMessageChan(topic string, partition int, offset int64) chan kafka.Message {
	messageChan := make(chan kafka.Message)
	logger := logrus.WithFields(logrus.Fields{"topic": topic, "partition": partition})

	g.gen.Start(func(ctx context.Context) {
		defer close(messageChan)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        g.brokers,
			Topic:          topic,
			Partition:      partition,
			MinBytes:       10e3,
			MaxBytes:       10e6,
			SessionTimeout: g.timeout,
			Dialer:         g.Dialer,
		})
		defer reader.Close()

		if err := reader.SetOffset(offset); err != nil {
			logger.WithError(err).Error("set offset")
			return
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, kafka.ErrGenerationEnded) && !errors.Is(err, io.EOF) {
				logger.WithError(err).Error("fetch message to initialize partitionProcessedOffsets")
			}
			return
		}

		// due to the reader is started we need to register the corresponding partition
		g.partitionRegistrationChan <- msg

		for {
			select {
			case <-ctx.Done(): // graceful shutdown + retry-generation
				return
			case messageChan <- msg:
			}

			msg, err = reader.FetchMessage(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, kafka.ErrGenerationEnded) && !errors.Is(err, io.EOF) {
					logger.WithError(err).Error("fetch message")
				}
				return
			}
		}
	})

	return messageChan
}

// runCommitLoop collects processed offsets and commits them by tick
func (g *generation) runCommitLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done(): // graceful shutdown
			g.commitOffsets()
			close(g.commitLoopStopped)
			return
		case <-ticker.C:
			g.commitOffsets()
		case registrationMessage := <-g.partitionRegistrationChan:
			g.registerCommittingOffsetPartition(registrationMessage)
		case message := <-g.processedMessageChan:
			g.collectProcessedMessage(message)
		}
	}
}

// registerCommittingOffsetPartition registers partition to commit offsets
func (g *generation) registerCommittingOffsetPartition(registrationMessage kafka.Message) {
	g.processedRecords[registrationMessage.Topic][registrationMessage.Partition] = &partitionProcessedOffsets{
		NextOffset:       registrationMessage.Offset,
		ProcessedOffsets: queue.New(),
	}
}

// collectProcessedMessage shifts offsets of particular partition if all offsets before were collected
//
//	partitionProcessedOffsets.NextOffset - initialize with the first fetched message generation.go:138
//	partitionProcessedOffsets.ProcessedOffsets - priority queue of collected offsets
//
//	NextOffset	|		ProcessedOffsets		|	processedMessageChan	|		comment
//		23		|								|							|	initialize partitionProcessedOffsets with NextOffset of particular partition
//													↓
//		23		|								|	27, 24, 23, 26			|	start collecting processed messages
//					 ┏------------------------------┛
//		23		|	27							|	24, 23, 26, 25			|	pivot is more than NextOffset -> add to ProcessedOffsets
//													↓
//		23		|	27							|	24, 23, 26, 25			|	next processed message
//					 ┏------------------------------┛
//		23		|	24, 27						|	23, 26, 25, 28			|	pivot is more than NextOffset -> add to ProcessedOffsets
//													↓
//		23		|	24, 27						|	23, 26, 25, 28			|	next processed message
//		 ┏------------------------------------------┛
//		23		|	24, 27						|	26, 25, 28				|	pivot is equal to NextOffset -> increment NextOffset -> check ProcessedOffsets
//					↓
//		24		|	24, 27						|	26, 25, 28				|	start checking ProcessedOffsets
//		 ┏----------┛
//		24		|	27							|	26, 25, 28				|	pivot is equal to NextOffset -> increment NextOffset -> check ProcessedOffsets
//					↓
//		25		|	27							|	26, 25, 28				|	pivot is more than NextOffset -> return to processedMessageChan
//													↓
//		25		|	27							|	26, 25, 28				|	next processed message...
func (g *generation) collectProcessedMessage(message kafka.Message) {
	if records, ok := g.processedRecords[message.Topic][message.Partition]; ok {
		if records.NextOffset == message.Offset {
			records.NextOffset++

			for records.ProcessedOffsets.Root() == records.NextOffset {
				records.ProcessedOffsets.Pop()
				records.NextOffset++
			}
		} else {
			records.ProcessedOffsets.Push(message.Offset)
		}
	} else {
		logrus.WithFields(logrus.Fields{"topic": message.Topic, "partition": message.Partition}).Warn("can't find partitionProcessedOffsets")
	}
}

func (g *generation) commitOffsets() {
	offsets := make(map[string]map[int]int64)
	committingRecords := make([]*partitionProcessedOffsets, 0)

	for topic, partitions := range g.processedRecords {
		partitionsOffsets := make(map[int]int64)
		for partition, records := range partitions {
			if records.CommittedOffset != records.NextOffset {
				partitionsOffsets[partition] = records.NextOffset
				committingRecords = append(committingRecords, records)
			}
		}

		if len(partitionsOffsets) > 0 {
			offsets[topic] = partitionsOffsets
		}
	}

	if len(offsets) > 0 {
		if err := g.gen.CommitOffsets(offsets); err != nil {
			logrus.WithError(err).Warn("commit offsets failed")
			return
		}
		logrus.Info("commit offsets succeeded")

		for _, record := range committingRecords {
			record.CommittedOffset = record.NextOffset
		}
	}
}

type partitionProcessedOffsets struct {
	// NextOffset offset that need to be committed in the next tick
	NextOffset int64
	// CommittedOffset offset that is already committed
	CommittedOffset int64
	// ProcessedOffsets ordered cache of processed offsets that need to be committed
	ProcessedOffsets *queue.Queue
}

func (g *generation) retry(ctx context.Context, consumer Consumer, m HeaderMap, message kafka.Message, err error) bool {
	attempt := m.GetAttempt()

	if continuer, hasContinuer := consumer.(Continuer); hasContinuer {
		param := ContinueParam{
			Message: message,
			Error:   err,
			Attempt: attempt,
		}

		doContinue := continuer.Continue(ctx, param)
		if !doContinue {
			return true
		}
	}

	if next, ok := g.nextRetryTopicMap[message.Topic]; ok {
		m.setAttempt(attempt + 1)
		nextRetryAt := time.Now().Add(next.delay)
		m.setRetryAt(nextRetryAt)

		message.Headers = m.ToHeaders()
		if err := g.producer.WriteMessages(ctx, kafka.Message{
			Topic:   next.topic,
			Key:     message.Key,
			Value:   message.Value,
			Headers: m.ToHeaders(),
		}); err != nil {
			logrus.WithError(err).WithField("topic", next.topic).Warn("send retry failed")
			return false
		}
	}

	return true
}
