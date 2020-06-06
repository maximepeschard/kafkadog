package client

import (
	"fmt"
	"strconv"
	"strings"
)

// Valid format string placeholders.
const (
	TopicPlaceholder     string = "%t"
	PartitionPlaceholder string = "%p"
	OffsetPlaceholder    string = "%o"
	TimestampPlaceholder string = "%e"
	KeyPlaceholder       string = "%k"
	ValuePlaceholder     string = "%v"
)

// FormatterHelp returns a message describing format tokens.
func FormatterHelp() string {
	var b strings.Builder
	fmt.Fprint(&b, "* available format tokens :\n")
	fmt.Fprintf(&b, "%s : topic\n", TopicPlaceholder)
	fmt.Fprintf(&b, "%s : partition\n", PartitionPlaceholder)
	fmt.Fprintf(&b, "%s : offset\n", OffsetPlaceholder)
	fmt.Fprintf(&b, "%s : timestamp\n", TimestampPlaceholder)
	fmt.Fprintf(&b, "%s : message key\n", KeyPlaceholder)
	fmt.Fprintf(&b, "%s : message value\n", ValuePlaceholder)
	fmt.Fprint(&b, "* example format string : 'received %v from %t'")
	return b.String()
}

// Formatter describes a way of printing Kafka messages.
type Formatter struct {
	format string
}

// NewFormatter returns a Formatter initialized with a format string.
func NewFormatter(format string) Formatter {
	return Formatter{format}
}

// Format returns a copy of the Formatter format string with the placeholders replaced with values from the Message.
func (f Formatter) Format(m Message) string {
	r := strings.NewReplacer(
		TopicPlaceholder, m.Topic,
		PartitionPlaceholder, strconv.FormatInt(int64(m.Partition), 10),
		OffsetPlaceholder, strconv.FormatInt(m.Offset, 10),
		TimestampPlaceholder, strconv.FormatInt(m.Timestamp.Unix(), 10),
		KeyPlaceholder, string(m.Key),
		ValuePlaceholder, string(m.Value),
	)
	return r.Replace(f.format)
}
