package client

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
)

// A ConsumerRequest specifies what to consume and how.
type ConsumerRequest struct {
	topic     string
	start     int64
	end       int64
	startTime *time.Time
	endTime   *time.Time
}

// NewConsumerRequest creates a valid ConsumerRequest.
func NewConsumerRequest(topic string, start int64, end int64) (ConsumerRequest, error) {
	cr := ConsumerRequest{topic: topic}

	now := time.Now()
	var startTime, endTime *time.Time

	if start == StartNow {
		startTime = &now
		cr.start = sarama.OffsetNewest
	} else if start == StartOldest {
		cr.start = sarama.OffsetOldest
	} else {
		t := time.Unix(start/1000, (start%1000)*1000000)
		startTime = &t
		cr.start = start
	}

	if end == EndNow {
		endTime = &now
	} else if end != EndNever {
		t := time.Unix(end/1000, (end%1000)*1000000)
		endTime = &t
	}
	cr.end = end

	if end != EndNever && start != StartOldest {
		if endTime.Before(*startTime) || endTime.Equal(*startTime) {
			return cr, errors.New("invalid start / end combination")
		}
	}

	cr.startTime = startTime
	cr.endTime = endTime

	return cr, nil
}

// Consumer is a high level API for a Kafka consumer.
type Consumer struct {
	broker   string
	client   sarama.Client
	consumer sarama.Consumer
}

// NewConsumer returns a Consumer for the given broker.
func NewConsumer(broker string) (Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	client, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
		return Consumer{}, err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return Consumer{}, err
	}

	return Consumer{broker, client, consumer}, nil
}

// Close shuts the consumer down.
func (c Consumer) Close() error {
	err := c.consumer.Close()
	err = c.client.Close()

	return err
}

// Consume reads messages matching the ConsumerRequest and sends them to the given chan until ctx is done.
func (c Consumer) Consume(ctx context.Context, req ConsumerRequest, messages chan<- Message) error {
	partitions, err := c.consumer.Partitions(req.topic)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	// We need to read separately each partition of the topic.
	for _, p := range partitions {
		partReq := partitionConsumerRequest{req, p}
		g.Go(func() error { return c.consumePartition(ctx, partReq, messages) })
	}

	return g.Wait()
}

type partitionConsumerRequest struct {
	ConsumerRequest

	partition int32
}

func (c Consumer) consumePartition(ctx context.Context, req partitionConsumerRequest, messages chan<- Message) error {
	startOffset, err := c.client.GetOffset(req.topic, req.partition, req.start)
	if err != nil {
		return err
	}

	pc, err := c.consumer.ConsumePartition(req.topic, req.partition, startOffset)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		pc.AsyncClose()
	}()

	for m := range pc.Messages() {
		message := Message{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     string(m.Value),
		}

		if req.endTime != nil && (m.Timestamp.After(*req.endTime) || pc.HighWaterMarkOffset() == m.Offset+1) {
			pc.AsyncClose()
			break
		}

		messages <- message
	}

	return nil
}
