package consumer

import (
	"context"
	"encoding/json"
	"github.com/iowanobos/kafka-custom-offset/consumer/queue"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	brokers              []string
	topic                string
	group                *kafka.ConsumerGroup
	generation           *kafka.Generation
	partitions           map[int]*ProcessedRecords
	processedMessageChan chan kafka.Message
}

func New(brokers []string, groupID, topic string) *Consumer {
	consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:          groupID,
		Brokers:     brokers,
		Topics:      []string{topic},
		StartOffset: kafka.FirstOffset,
		// TODO: Проверить GroupBalancers
	})
	if err != nil {
		log.Fatalln(err)
	}

	return &Consumer{
		brokers:              brokers,
		topic:                topic,
		group:                consumerGroup,
		partitions:           make(map[int]*ProcessedRecords),
		processedMessageChan: make(chan kafka.Message),
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

func (c *Consumer) Consume(ctx context.Context) (map[int]<-chan kafka.Message, chan<- kafka.Message) {
	generation, err := c.group.Next(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	c.generation = generation

	partitions, ok := generation.Assignments[c.topic]
	if !ok {
		log.Fatalln("топик не найден")
	}

	fetchedMessageChanByPartition := make(map[int]<-chan kafka.Message, len(partitions))

	var wg sync.WaitGroup
	for _, partition := range partitions {
		messageChan := make(chan kafka.Message)
		fetchedMessageChanByPartition[partition.ID] = messageChan

		log.Printf("Start consuming. Partition: %d. Offset: %d", partition.ID, partition.Offset)

		id := partition.ID
		offset := partition.Offset

		wg.Add(1)
		c.generation.Start(func(ctx context.Context) {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   c.brokers,
				Topic:     c.topic,
				MinBytes:  10e3, // 10KB
				MaxBytes:  10e6, // 10MB
				Partition: id,
			})
			defer reader.Close()

			if err := reader.SetOffset(offset); err != nil {
				log.Println("set offset failed. error: ", err.Error())
				wg.Done()
				return
			}

			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Println("fetch message failed. error: ", err.Error())
				wg.Done()
				return
			}

			c.partitions[id] = &ProcessedRecords{
				NextOffset:       msg.Offset,
				ProcessedOffsets: queue.New(),
			}
			wg.Done()
			messageChan <- msg

			for {
				msg, err = reader.FetchMessage(ctx)
				if err != nil {
					log.Println("fetch message failed. error: ", err.Error())
					return
				}
				log.Printf("Fetch. Value: %s. Partition: %d. Offset: %d\n", string(msg.Value), msg.Partition, msg.Offset)

				select {
				case <-ctx.Done():
					log.Println("fetch shutdown")
					close(messageChan)
					return
				case messageChan <- msg:
				}
			}
		})
	}

	wg.Wait()
	c.runCommitLoop(ctx)

	return fetchedMessageChanByPartition, c.processedMessageChan
}

func (c *Consumer) runCommitLoop(ctx context.Context) {
	offsets := map[string]map[int]int64{
		c.topic: make(map[int]int64, len(c.partitions)),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				log.Println("runCommitLoop shutdown")
				c.flushOffsets(offsets)
				return
			case <-ticker.C:
				c.flushOffsets(offsets)
			case record := <-c.processedMessageChan:
				processedRecords, ok := c.partitions[record.Partition]
				if ok {

					{
						// TODO: Удалить
						log.Printf("Processed. Partition: %d. Offset: %d. NextOffset: %d\n", record.Partition, record.Offset, processedRecords.NextOffset)
					}

					processedRecords.Lock()
					if processedRecords.NextOffset == record.Offset {
						processedRecords.NextOffset++

						for processedRecords.ProcessedOffsets.Root() == processedRecords.NextOffset {
							processedRecords.ProcessedOffsets.Pop()
							processedRecords.NextOffset++
						}

					} else {
						processedRecords.ProcessedOffsets.Push(record.Offset)
					}
					processedRecords.Unlock()
				}
			}
		}
	}()
}

func (c *Consumer) flushOffsets(offsets map[string]map[int]int64) {
	for _, partitions := range offsets {
		for partition, records := range c.partitions {
			partitions[partition] = records.NextOffset
		}
	}

	{
		// TODO: Удалить
		data, _ := json.Marshal(offsets)
		log.Println("flush offsets: ", string(data))
	}

	if err := c.generation.CommitOffsets(offsets); err != nil {
		log.Printf("commit offsets failed. error: %s\n", err.Error())
	}
}

type ProcessedRecords struct {
	sync.Mutex
	NextOffset       int64
	ProcessedOffsets *queue.Queue
}
