package client

import (
	"github.com/Shopify/sarama"
)

// Producer is a high level API for a Kafka producer.
type Producer struct {
	broker   string
	client   sarama.Client
	producer sarama.SyncProducer
}

// NewProducer returns a Producer for the given broker.
func NewProducer(broker string) (Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	client, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
		return Producer{}, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return Producer{}, err
	}

	return Producer{broker, client, producer}, nil
}

// Produce sends a Message to Kafka.
func (p Producer) Produce(m Message) error {
	pm := &sarama.ProducerMessage{Topic: m.Topic, Value: sarama.StringEncoder(m.Value)}
	if m.Key != "" {
		pm.Key = sarama.StringEncoder(m.Key)
	}
	_, _, err := p.producer.SendMessage(pm)

	return err
}

// Close shuts the producer down.
func (p Producer) Close() error {
	err := p.producer.Close()
	err = p.client.Close()

	return err
}
