package gossamer

import (
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// RetryHeader custom headers for retry
type RetryHeader = string

const (
	// RetryHeaderAttempt is attempt number
	RetryHeaderAttempt RetryHeader = "X-Retry-Attempt"
	// RetryHeaderRetryAt is when a message will be consumed next time(time.RFC3339Nano)
	RetryHeaderRetryAt RetryHeader = "X-Retry-RetryAt"
)

type HeaderMap map[string][]byte

func HeadersToMap(headers []kafka.Header) HeaderMap {
	m := make(HeaderMap, len(headers))
	for _, header := range headers {
		m[header.Key] = header.Value
	}
	return m
}

func (m HeaderMap) ToHeaders() []kafka.Header {
	headers := make([]kafka.Header, 0, len(m))
	for key, value := range m {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: value,
		})
	}
	return headers
}

func (m HeaderMap) GetAttempt() int {
	attempt, _ := strconv.Atoi(string(m[RetryHeaderAttempt]))
	return attempt
}

func (m HeaderMap) setAttempt(attempt int) {
	m[RetryHeaderAttempt] = []byte(strconv.Itoa(attempt))
}

func (m HeaderMap) GetRetryAt() (time.Time, bool) {
	value, ok := m[RetryHeaderRetryAt]
	if !ok {
		return time.Time{}, false
	}

	retryAt, err := time.Parse(time.RFC3339Nano, string(value))
	if err != nil {
		return time.Time{}, false
	}

	return retryAt, true
}

func (m HeaderMap) setRetryAt(retryAt time.Time) {
	m[RetryHeaderRetryAt] = []byte(retryAt.Format(time.RFC3339Nano))
}
