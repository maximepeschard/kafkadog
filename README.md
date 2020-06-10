[![Release](https://img.shields.io/github/v/release/maximepeschard/kafkadog.svg)](https://github.com/maximepeschard/kafkadog/releases/latest)
[![Release](https://github.com/maximepeschard/kafkadog/workflows/Release/badge.svg)](https://github.com/maximepeschard/kafkadog/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maximepeschard/kafkadog)](https://goreportcard.com/report/github.com/maximepeschard/kafkadog)

# kafkadog

A Kafka consumer/producer CLI. Barking version of [Kafkacat](https://github.com/edenhill/kafkacat).

## Installation

### Homebrew

```console
$ brew install maximepeschard/tap/kafkadog
```

### go get

```console
$ go get github.com/maximepeschard/kafkadog
```

### Binary

Grab the [latest release](https://github.com/maximepeschard/kafkadog/releases/latest) for your platform.

## Example usage

### Consumer

Read messages and print their value on stdout :

```console
$ kafkadog consume -b mybroker:9092 my_topic
```

Read messages and print them with the format `topic.partition.offset -> value` on stdout :

```console
$ kafkadog consume -b mybroker:9092 -f '%t.%p.%o -> %v' my_topic
```

Read messages starting at the oldest available offset and print their value on stdout :

```console
$ kafkadog consume -b mybroker:9092 -s oldest my_topic
```

Read messages starting from a date (in UTC timezone) and print their value on stdout :

```console
$ kafkadog consume -b mybroker:9092 -s '2020-06-10 00:00:00' my_topic
```

Read messages starting from a UNIX timestamp (in milliseconds) and print their value on stdout :

```console
$ kafkadog consume -b mybroker:9092 -s 1591747200000 my_topic
```

### Producer

Produce one message :

```console
$ echo '{"msg": "hello"}' | kafkadog produce -b mybroker:9092 my_topic
```

Produce messages read from stdin (default is one message per line, close with Ctrl-D / Ctrl-C) :

```console
$ kafkadog produce -b mybroker:9092 my_topic
```

Produce messages read from a file (default is one message per line) :

```console
$ kafkadog produce -b mybroker:9092 -f messages.txt my_topic
```

## Detailed usage

Run `kafkadog -h` for detailed usage and help.

## Credits

* [Shopify/sarama](https://github.com/Shopify/sarama)
* [spf13/cobra](https://github.com/spf13/cobra)