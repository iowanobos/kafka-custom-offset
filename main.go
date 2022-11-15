package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/iowanobos/kafka-custom-offset/consumer"
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	ctx := context.TODO()

	cfg := new(config)
	envconfig.MustProcess(configPrefix, cfg)
	if cfg.Coefficient <= 0 {
		log.Fatalln("rate должен быть не меньше или равен 0")
	}

	initTopic(ctx, cfg)
	go RunProducer(ctx, cfg)
	RunConsumer(ctx, cfg)
}

func initTopic(ctx context.Context, cfg *config) {
	brokers := strings.Split(cfg.Brokers, ",")
	if len(brokers) == 0 {
		log.Fatalln("адреса брокеров не указаны")
	}
	for _, broker := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			log.Fatalln("dial kafka failed. error: ", err.Error())
		}

		if err = conn.CreateTopics(kafka.TopicConfig{Topic: cfg.Topic, NumPartitions: 12, ReplicationFactor: 3}); err != nil {
			log.Println("create topic failed. error: ", err.Error())
		} else {
			log.Println("create topic succeeded")
			break
		}
	}
}

func RunProducer(ctx context.Context, cfg *config) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(cfg.Brokers, ",")...),
		Topic:    cfg.Topic,
		Balancer: new(kafka.LeastBytes),
	}

	ticker := time.NewTicker(time.Millisecond * (1 + time.Duration(rand.Intn(int(10*cfg.Coefficient)))))
	for range ticker.C {
		go func() {
			id := uuid.New().String()

			if err := w.WriteMessages(ctx, kafka.Message{Value: []byte(id)}); err != nil {
				log.Fatalln("write message failed. error: ", err.Error())
			}
			fmt.Printf("Write. Value: %s\n", id)
		}()
	}
}

func RunConsumer(ctx context.Context, cfg *config) {
	fetchedMessageChanByPartition, processedMessageChan := consumer.
		New(strings.Split(cfg.Brokers, ","), cfg.GroupID, cfg.Topic).
		Consume(ctx)

	var wg sync.WaitGroup
	for _, fetchedMessageChan := range fetchedMessageChanByPartition {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int, fetchedMessageChan <-chan kafka.Message) {
				defer wg.Done()

				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-fetchedMessageChan:
						fmt.Printf("Read %d. Value: %s. Partition: %d. Offset: %d\n", i, string(msg.Value), msg.Partition, msg.Offset)
						time.Sleep(time.Millisecond * (100 + time.Duration(rand.Intn(int(300*cfg.Coefficient)))))
						processedMessageChan <- msg
					}
				}
			}(i, fetchedMessageChan)
		}
	}

	wg.Wait()
}
