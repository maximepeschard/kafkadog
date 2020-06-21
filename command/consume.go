package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/maximepeschard/kafkadog/client"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringP("broker", "b", "localhost:9092", "bootstrap broker")
	consumeCmd.Flags().StringP("start", "s", "now", "when to start reading from :\n"+client.StartHelp())
	consumeCmd.Flags().StringP("end", "e", "never", "when to stop reading :\n"+client.EndHelp())
	consumeCmd.Flags().StringP("format", "f", client.ValuePlaceholder, "message output format, using format tokens :\n"+client.FormatterHelp())
}

var consumeCmd = &cobra.Command{
	Use:   "consume [topic]",
	Short: "Consume messages from Kafka.",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := args[0]
		broker, err := cmd.Flags().GetString("broker")
		if err != nil {
			return err
		}
		startStr, err := cmd.Flags().GetString("start")
		if err != nil {
			return err
		}
		endStr, err := cmd.Flags().GetString("end")
		if err != nil {
			return err
		}
		format, err := cmd.Flags().GetString("format")
		if err != nil {
			return err
		}

		start, err := client.ParseStart(startStr)
		if err != nil {
			return err
		}
		end, err := client.ParseEnd(endStr)
		if err != nil {
			return err
		}
		formatter := client.NewFormatter(format)

		req, err := client.NewConsumerRequest(topic, start, end)
		if err != nil {
			return err
		}

		// At this point, input is validated and errors should not display usage.
		rootCmd.SilenceUsage = true

		c, err := client.NewConsumer(broker)
		if err != nil {
			return err
		}
		defer func() {
			if err := c.Close(); err != nil {
				fmt.Println("can't close consumer")
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

		go func() {
			err = c.Consume(ctx, req, messages)
			close(messages)
		}()

		for m := range messages {
			fmt.Println(formatter.Format(m))
		}

		return err
	},
}
