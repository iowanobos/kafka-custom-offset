package gossamer

import (
	"time"
)

// Options configures Group
type Options struct {
	Brokers        []string
	Group          string
	Topic          string
	StartOffset    int64
	SessionTimeout time.Duration
	TLS            *TlsOptions

	// RetryTopics defines delays for sequential consuming retrying messages
	RetryTopics []RetryTopicOption
	// PartitionWorkerCount count of workers launching per each partition
	PartitionWorkerCount int
}

type RetryTopicOption struct {
	Delay time.Duration
	Topic string
}

type TlsOptions struct {
	ClientID string
	CA       string
	Cert     string
	Key      string
}
