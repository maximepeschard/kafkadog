package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

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

// Consume reads messages from topic and sends them to the given chan until ctx is done.
func (c Consumer) Consume(ctx context.Context, topic string, startTime int64, messages chan<- Message) error {
	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// We need to read separately each partition of the topic.
	for _, p := range partitions {
		startOffset, err := c.client.GetOffset(topic, p, startTime)
		if err != nil {
			return err
		}

		pc, err := c.consumer.ConsumePartition(topic, p, startOffset)
		if err != nil {
			fmt.Println(err)
			return err
		}

		go func(pc sarama.PartitionConsumer) {
			<-ctx.Done()
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()

			for m := range pc.Messages() {
				message := Message{
					Topic:     m.Topic,
					Partition: m.Partition,
					Offset:    m.Offset,
					Timestamp: m.Timestamp,
					Key:       string(m.Key),
					Value:     string(m.Value),
				}
				messages <- message
			}
		}(pc)
	}

	wg.Wait()
	return nil
}

// Close shuts the consumer down.
func (c Consumer) Close() error {
	err := c.consumer.Close()
	err = c.client.Close()

	return err
}
