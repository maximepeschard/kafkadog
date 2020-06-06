package command

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/maximepeschard/kafkadog/client"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(produceCmd)
	produceCmd.Flags().StringP("broker", "b", "localhost:9092", "bootstrap broker")
	produceCmd.Flags().StringP("file", "f", "", "messages file")
	produceCmd.Flags().StringP("delimiter", "d", "\n", "messages delimiter")
	produceCmd.Flags().StringP("key delimiter", "D", "", "key-message delimiter")
}

var produceCmd = &cobra.Command{
	Use:   "produce [topic]",
	Short: "Produce messages to Kafka.",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := args[0]
		broker, err := cmd.Flags().GetString("broker")
		if err != nil {
			return err
		}
		filename, err := cmd.Flags().GetString("file")
		if err != nil {
			return err
		}
		delim, err := cmd.Flags().GetString("delimiter")
		if err != nil {
			return err
		}
		keyDelim, err := cmd.Flags().GetString("key delimiter")
		if err != nil {
			return err
		}

		p, err := client.NewProducer(broker)
		if err != nil {
			return err
		}
		defer func() {
			if err := p.Close(); err != nil {
				fmt.Println("can't close producer")
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
			<-quit
			cancel()
		}()

		messages := make(chan client.Message, client.MessageBufferSize)

		var r io.Reader
		if filename != "" {
			f, err := os.Open(filename)
			if err != nil {
				return err
			}
			r = f
			defer func() {
				if err := f.Close(); err != nil {
					fmt.Println("can't close file")
				}
			}()
		} else {
			r = os.Stdin
		}

		// NB: This goroutine will leak if ctx is canceled before it finishes reading
		//     but we don't care since in that case we exit right after.
		go func() {
			read(topic, r, delim, keyDelim, messages)
			close(messages)
		}()

		return produce(ctx, p, messages)
	},
}

func read(topic string, r io.Reader, delim string, keyDelim string, messages chan<- client.Message) {
	scanner := bufio.NewScanner(r)
	splitOnDelim := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if i := strings.Index(string(data), delim); i >= 0 {
			return i + 1, data[:i], nil
		}

		return 0, nil, nil
	}
	scanner.Split(splitOnDelim)

	for scanner.Scan() {
		messages <- parseMessage(scanner.Text(), topic, keyDelim)
	}
}

func produce(ctx context.Context, p client.Producer, messages <-chan client.Message) error {
	for {
		select {
		case m, open := <-messages:
			if !open {
				return nil
			}
			if err := p.Produce(m); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func parseMessage(s string, topic string, keyDelim string) client.Message {
	if keyDelim != "" {
		parts := strings.SplitN(s, keyDelim, 2)
		if len(parts) > 1 {
			return client.Message{Topic: topic, Key: parts[0], Value: parts[1]}
		}
	}

	return client.Message{Topic: topic, Value: s}
}
