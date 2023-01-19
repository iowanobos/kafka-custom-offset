package gossamer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	libName               = "gossamer"
	reconnectTimeoutInSec = 3
)

// Group provides easy-to-use consuming messages
type Group struct {
	config

	// core
	group *kafka.ConsumerGroup

	// graceful shutdown
	gracefulShutdownCtx context.Context
	gracefulShutdown    func()
	groupStopped        chan struct{}
}

type config struct {
	brokers           []string
	workerCount       int
	timeout           time.Duration
	nextRetryTopicMap map[string]nextTopic
	TLS               *TlsOptions
	Dialer            *kafka.Dialer
}

type nextTopic struct {
	topic string
	delay time.Duration
}

// NewGroup creates Group
func NewGroup(options Options) (*Group, error) {
	topics := make([]string, 0, 1+len(options.RetryTopics))
	nextRetryTopicMap := getNextRetryTopicMap(options)
	if len(nextRetryTopicMap) > 0 {
		for topic := range nextRetryTopicMap {
			topics = append(topics, topic)
		}
	} else {
		topics = append(topics, options.Topic)
	}

	if options.PartitionWorkerCount == 0 {
		options.PartitionWorkerCount = 1
	}

	cfg := kafka.ConsumerGroupConfig{
		ID:             options.Group,
		Brokers:        options.Brokers,
		Topics:         topics,
		StartOffset:    options.StartOffset,
		SessionTimeout: options.SessionTimeout,
	}

	if options.TLS != nil {
		tlsConfig, err := getTlsConfig(options.TLS)
		if err != nil {
			return nil, err
		}

		cfg.Dialer = &kafka.Dialer{
			ClientID:  options.TLS.ClientID,
			Timeout:   10 * time.Second,
			DualStack: true,
			TLS:       tlsConfig,
		}
	}

	group, err := kafka.NewConsumerGroup(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Group{
		config: config{
			brokers:           options.Brokers,
			workerCount:       options.PartitionWorkerCount,
			timeout:           options.SessionTimeout,
			nextRetryTopicMap: nextRetryTopicMap,
			TLS:               options.TLS,
			Dialer:            cfg.Dialer,
		},

		group: group,

		gracefulShutdownCtx: ctx,
		gracefulShutdown:    cancel,
		groupStopped:        make(chan struct{}),
	}, nil
}

// getNextRetryTopicMap defines the map where a key is the current topic and a value is the next topic where will be sent retry message
func getNextRetryTopicMap(options Options) map[string]nextTopic {
	delayTopicsNumber := len(options.RetryTopics)
	if delayTopicsNumber == 0 {
		return nil
	}

	nextByTopic := make(map[string]nextTopic, 1+delayTopicsNumber)

	currentTopic := options.Topic
	for _, retryTopic := range options.RetryTopics {
		nextByTopic[currentTopic] = nextTopic{
			topic: retryTopic.Topic,
			delay: retryTopic.Delay,
		}
		currentTopic = retryTopic.Topic
	}

	// the last node points to itself
	nextByTopic[currentTopic] = nextTopic{
		topic: currentTopic,
		delay: options.RetryTopics[delayTopicsNumber-1].Delay,
	}

	return nextByTopic
}

func getTlsConfig(options *TlsOptions) (*tls.Config, error) {
	if options.CA != "" && options.Cert != "" && options.Key != "" {
		// Keystore
		crt, err := tls.X509KeyPair([]byte(options.Cert), []byte(options.Key))
		if err != nil {
			return nil, err
		}

		// Truststore
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(options.CA))
		return &tls.Config{
			Certificates:       []tls.Certificate{crt},
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}, nil
	}

	return nil, nil
}

// Run starts your Consumer.
// Supports reconnections, graceful shutdown, custom offsets, retries
func (g *Group) Run(consumer Consumer) {
	defer func() {
		close(g.groupStopped)
	}()

	for {
		logrus.Debug("try to connect")

		gen, err := g.newGeneration()
		if err != nil {
			if errors.Is(err, context.Canceled) { // graceful shutdown
				return
			}
			logrus.WithError(err).Warn("connect failed")
		} else {
			logrus.Info("connection succeeded")
			gen.Run(g.gracefulShutdownCtx, consumer)
		}

		select {
		case <-g.gracefulShutdownCtx.Done(): // graceful shutdown
			logrus.Info("generation shutdown succeeded")
			return
		case <-time.After(reconnectTimeoutInSec * time.Second): // wait before reconnect
		}
	}
}

// Close gracefully closes Group
func (g *Group) Close() error {
	g.gracefulShutdown()
	<-g.groupStopped
	return g.group.Close()
}
