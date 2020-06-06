package client

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// ParseStart returns a start offset parsed from s.
func ParseStart(s string) (int64, error) {
	switch s {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		return 0, fmt.Errorf("invalid start offset %q", s)
	}
}
