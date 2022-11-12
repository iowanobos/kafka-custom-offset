package main

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/segmentio/kafka-go"
)

const (
	configPrefix = "TEST"
)

type config struct {
	GroupID     string  `envconfig:"GROUP_ID" default:"groupID"`
	Brokers     string  `envconfig:"BROKERS" default:"localhost:9091,localhost:9092,localhost:9093"`
	Topic       string  `envconfig:"TOPIC" default:"test"`
	Coefficient float64 `envconfig:"COEFFICIENT" default:"1"`
}

func main() {
	ctx := context.TODO()

	cfg := new(config)
	envconfig.MustProcess(configPrefix, cfg)
	if cfg.Coefficient <= 0 {
		log.Fatalln("rate должен быть не меньше или равен 0")
	}

	go RunConsumer(ctx, cfg)
	RunProducer(ctx, cfg)
}

func RunConsumer(ctx context.Context, cfg *config) {
	consumer := NewConsumer(cfg)
	fetchedMessageChanByPartition, processedMessageChan := consumer.Consume(ctx)

	var wg sync.WaitGroup
	for _, fetchedMessageChan := range fetchedMessageChanByPartition { // TODO: Нужно спросить в целом какой API сделать для либы
		fetchedMessageChan := fetchedMessageChan
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-fetchedMessageChan:
						Sleep(cfg.Coefficient)
						fmt.Printf("Read. Value: %s. Partition: %d. Offset: %d\n", string(msg.Value), msg.Partition, msg.Offset)
						processedMessageChan <- msg
					}
				}
			}()
		}
	}

	wg.Wait()
}

func RunProducer(ctx context.Context, cfg *config) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(cfg.Brokers, ",")...),
		Topic:    cfg.Topic,
		Balancer: new(kafka.LeastBytes),
	}

	for {
		id := uuid.New().String()

		if err := w.WriteMessages(ctx,
			kafka.Message{
				Value: []byte(id),
			},
		); err != nil {
			log.Fatalln("write message failed. error: ", err.Error())
		}
		fmt.Printf("Write. Value: %s\n", id)
		Sleep(cfg.Coefficient)
	}
}

func Sleep(coef float64) {
	ms := time.Millisecond*10 + time.Duration(rand.Intn(int(100*coef)))
	time.Sleep(ms)
}

type Consumer struct {
	brokers              []string
	topic                string
	group                *kafka.ConsumerGroup
	generation           *kafka.Generation
	partitions           map[int]*ProcessedRecords
	processedMessageChan chan kafka.Message
}

func NewConsumer(cfg *config) *Consumer {
	brokers := strings.Split(cfg.Brokers, ",")
	consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:          cfg.GroupID,
		Brokers:     brokers,
		Topics:      []string{cfg.Topic},
		StartOffset: kafka.FirstOffset,
		// TODO: Проверить GroupBalancers
	})
	if err != nil {
		log.Fatalln(err)
	}

	return &Consumer{
		brokers:              brokers,
		topic:                cfg.Topic,
		group:                consumerGroup,
		partitions:           make(map[int]*ProcessedRecords),
		processedMessageChan: make(chan kafka.Message),
	}
}

func (c *Consumer) Consume(ctx context.Context) (map[int]chan kafka.Message, chan kafka.Message) {
	generation, err := c.group.Next(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	c.generation = generation

	partitions, ok := generation.Assignments[c.topic]
	if !ok {
		log.Fatalln("топик не найден")
	}

	//for _, partition := range partitions {
	//	c.partitions[partition.ID] = ProcessedRecords{
	//		NextOffset:       partition.Offset, // TODO: Оказывается приходит -1 и -2. Непонятно какой элемент ждать следующий
	//		ProcessedOffsets: list.New(),       // -1 видимо означает что нет данных о последних оффсетах коммитов и тогда хз где вообще начало / нужна логика на инициализацию лупа по первым сообщениям из партиций/ то есть первые сообщения из партиций при -1 будут инициализировать луп
	//	}
	//}

	fetchedMessageChanByPartition := make(map[int]chan kafka.Message, len(partitions))

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
				NextOffset:       msg.Offset, // TODO: Оказывается приходит -1 и -2. Непонятно какой элемент ждать следующий
				ProcessedOffsets: list.New(), // -1 видимо означает что нет данных о последних оффсетах коммитов и тогда хз где вообще начало / нужна логика на инициализацию лупа по первым сообщениям из партиций/ то есть первые сообщения из партиций при -1 будут инициализировать луп
			}
			wg.Done()
			messageChan <- msg

			for {
				msg, err = reader.FetchMessage(ctx)
				if err != nil {
					log.Println("fetch message failed. error: ", err.Error())
					return
				}
				fmt.Printf("Fetch. Value: %s. Partition: %d. Offset: %d\n", string(msg.Value), msg.Partition, msg.Offset)

				messageChan <- msg
			}
		})
	}

	wg.Wait()
	c.runCommitLoop(ctx)

	return fetchedMessageChanByPartition, c.processedMessageChan
}

func (c *Consumer) runCommitLoop(ctx context.Context) {
	partitions := make(map[int]int64, len(c.partitions))
	offsets := map[string]map[int]int64{
		c.topic: partitions,
	}

	go func() {
		for {
			ticker := time.NewTicker(time.Second)
			select {
			case <-ctx.Done():
				// TODO: понять как нужно выходить из цикла
			case <-ticker.C:
				for partition, records := range c.partitions {
					partitions[partition] = records.NextOffset
				}

				data, _ := json.Marshal(offsets)
				println(string(data))
				_ = c.generation.CommitOffsets(offsets) // TODO: понять что нужно делать с ошибкой
			case record := <-c.processedMessageChan:
				processedRecords, ok := c.partitions[record.Partition]
				if ok {
					fmt.Printf("Processed. Partition: %d. Offset: %d. NextOffset: %d\n", record.Partition, record.Offset, processedRecords.NextOffset)
					processedRecords.Lock()
					if processedRecords.NextOffset == record.Offset {
						processedRecords.NextOffset++

						for hasNextOffset(processedRecords.ProcessedOffsets, processedRecords.NextOffset) {
							processedRecords.NextOffset++
						}

					} else {
						insertionPush(processedRecords.ProcessedOffsets, record.Offset)
					}
					processedRecords.Unlock()
				}
			}
		}
	}()
}

func insertionPush(processedOffsets *list.List, offset int64) {
	elem := processedOffsets.Back()
	for {
		if elem == nil {
			processedOffsets.PushFront(offset)
			return
		}

		if value, ok := elem.Value.(int64); ok {
			if value > offset {
				elem = elem.Prev()
			} else {
				processedOffsets.InsertAfter(offset, elem)
				return
			}
		}
	}
}

func hasNextOffset(processedOffsets *list.List, offset int64) bool {
	elem := processedOffsets.Front()
	if elem != nil && elem.Value == offset {
		processedOffsets.Remove(elem)
		return true
	}
	return false
}

type ProcessedRecords struct {
	sync.Mutex
	NextOffset       int64
	ProcessedOffsets *list.List
}
