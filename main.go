package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/iowanobos/kafka-custom-offset/gossamer"
	"github.com/kelseyhightower/envconfig"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
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
	ctx, cancel := context.WithCancel(context.Background())

	cfg := new(config)
	envconfig.MustProcess(configPrefix, cfg)
	if cfg.Coefficient <= 0 {
		log.Fatalln("rate должен быть не меньше или равен 0")
	}

	initTopic(ctx, cfg)
	var eg errgroup.Group
	eg.Go(func() error {
		return RunProducer(ctx, cfg)
	})

	group, err := gossamer.NewGroup(gossamer.Options{
		Brokers:              strings.Split(cfg.Brokers, ","),
		Group:                cfg.GroupID,
		Topic:                cfg.Topic,
		PartitionWorkerCount: 100,
	})
	if err != nil {
		log.Fatalf("Create consumer group failed. Error: %v\n", err)
	}

	eg.Go(func() error {
		go group.Run(Consumer{cfg: cfg})
		<-ctx.Done()
		return group.Close()
	})

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	select {
	case <-sigc:
		log.Println("Start shutdowning")
		cancel()

		if err := eg.Wait(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Println("Shutdown error: ", err.Error())
			}
		}

	}
	log.Println("Application shut downing...")
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

func RunProducer(ctx context.Context, cfg *config) error {
	w := &kafka.Writer{
		Addr:  kafka.TCP(strings.Split(cfg.Brokers, ",")...),
		Topic: cfg.Topic,
	}

	ticker := time.NewTicker(time.Millisecond * (1 + time.Duration(rand.Intn(int(10*cfg.Coefficient)))))
	breakTicker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-breakTicker.C:
			time.Sleep(time.Second * 3)
		case <-ticker.C:
			go func() {
				id := uuid.New().String()

				if err := w.WriteMessages(ctx, kafka.Message{Value: []byte(id)}); err != nil {
					return
				}
				log.Printf("Write. Value: %s\n", id)
			}()
		}
	}

	return nil
}

type Consumer struct {
	cfg *config
}

func (c Consumer) Consume(ctx context.Context, message kafka.Message) error {
	log.Printf("Read. Value: %s. Partition: %d. Offset: %d\n", string(message.Value), message.Partition, message.Offset)
	processDuration := time.Millisecond * (100 + time.Duration(rand.Intn(int(300*c.cfg.Coefficient))))

	select {
	case <-time.After(processDuration):
	case <-ctx.Done():
	}
	return nil
}
