package client

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const dateTimeLayout string = "2006-01-02 15:04:05"

// Special time values
const (
	StartOldest int64 = -1
	StartNow    int64 = -2
	EndNow      int64 = -3
	EndNever    int64 = -4
)

// StartHelp returns a message describing valid start values.
func StartHelp() string {
	var b strings.Builder
	fmt.Fprintf(&b, "* now : start from messages more recent than now\n")
	fmt.Fprintf(&b, "* oldest : start at the oldest available offset\n")
	fmt.Fprintf(&b, "* YYYY-mm-dd HH:MM:SS : start at this date (UTC)\n")
	fmt.Fprintf(&b, "* <epoch in milliseconds> : start at this epoch")
	return b.String()
}

// ParseStart returns a start time parsed from s.
func ParseStart(s string) (int64, error) {
	switch s {
	case "oldest":
		return StartOldest, nil
	case "now":
		return StartNow, nil
	default:
		start, err := parseDateTime(s)
		if err == nil {
			return start, nil
		}

		start, err = parseEpoch(s)
		if err == nil {
			return start, nil
		}

		return 0, fmt.Errorf("invalid start time %q", s)
	}
}

// EndHelp returns a message describing valid end values.
func EndHelp() string {
	var b strings.Builder
	fmt.Fprintf(&b, "* now : end when messages are more recent than now\n")
	fmt.Fprintf(&b, "* YYYY-mm-dd HH:MM:SS : end at this date (UTC)\n")
	fmt.Fprintf(&b, "* <epoch in milliseconds> : end at this epoch")
	return b.String()
}

// ParseEnd returns an end time parsed from s.
func ParseEnd(s string) (int64, error) {
	switch s {
	case "now":
		return EndNow, nil
	case "never":
		return EndNever, nil
	default:
		end, err := parseDateTime(s)
		if err == nil {
			return end, nil
		}

		end, err = parseEpoch(s)
		if err == nil {
			return end, nil
		}

		return 0, fmt.Errorf("invalid end time %q", s)
	}
}

func parseDateTime(s string) (int64, error) {
	t, err := time.Parse(dateTimeLayout, s)
	if err != nil {
		return 0, err
	}

	return t.UnixNano() / int64(time.Millisecond), nil
}

func parseEpoch(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
