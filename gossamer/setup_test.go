package gossamer

import (
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
)

func TestMain(m *testing.M) {
	var code int
	defer os.Exit(code)

	containers := testcontainers.
		NewLocalDockerCompose([]string{"../docker-compose.yml"}, libName+"_test_setup").
		WithCommand([]string{"up", "-d", "zookeeper", "broker2"})
	containers.Invoke()
	defer containers.Down()

	ticker := time.NewTicker(time.Millisecond * 100)
	timeout := time.After(30 * time.Second)
loop:
	for {
		select {
		case <-ticker.C:
			if conn, err := kafka.Dial("tcp", broker); err == nil {
				if conn.CreateTopics(kafka.TopicConfig{
					Topic:             topic,
					NumPartitions:     1,
					ReplicationFactor: 1,
				}) == nil {
					break loop
				}
			}
		case <-timeout:
			break loop
		}
	}

	code = m.Run()
}
