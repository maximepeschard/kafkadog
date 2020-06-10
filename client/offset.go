package client

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const dateTimeLayout string = "2006-01-02 15:04:05"

// StartHelp returns a message describing valid start values.
func StartHelp() string {
	var b strings.Builder
	fmt.Fprint(&b, "* available start values :\n")
	fmt.Fprintf(&b, "newest : start at log head offset\n")
	fmt.Fprintf(&b, "oldest : start at the oldest available offset\n")
	fmt.Fprintf(&b, "YYYY-mm-dd HH:MM:SS : start at this date (UTC)\n")
	fmt.Fprintf(&b, "<epoch in milliseconds> : start at this epoch")
	return b.String()
}

// ParseStart returns a start time parsed from s.
func ParseStart(s string) (int64, error) {
	switch s {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		t, err := time.Parse(dateTimeLayout, s)
		if err == nil {
			return t.UnixNano() / int64(time.Millisecond), nil
		}

		e, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return e, nil
		}

		return 0, fmt.Errorf("invalid start offset %q", s)
	}
}
