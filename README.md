# Custom offset for Kafka

Consumer provides two channels:

- **message chan** - to get messages for processing
- **commit chan** - to send messages into a queue for committing (in correct order)

You can create pool of workers for each partition and listen **message chan**. After a successful processing of the
message you need to send this message to **commit chan**

Successful processing means that you made some logic and atomicity persist data with message offset in storage.
Persisting offset in storage is needed for deduplication, it guarantees not to process a message twice

## Get Started

1. `docker-compose up -d` to run kafka cluster locally
2. `go run main.go` to run application
3. open http://localhost:8080/ui/docker-kafka-server/topic
4. refresh the page and check the changing lag of your topic
