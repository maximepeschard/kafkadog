package client

import (
	"time"
)

// MessageBufferSize is the default buffer size for consuming/producing.
const MessageBufferSize = 256

// Message represents a Kafka message.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       string
	Value     string
}
