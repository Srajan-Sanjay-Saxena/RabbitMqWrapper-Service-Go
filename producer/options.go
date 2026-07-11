package producer

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProducerConfig struct {
	ExchangeName string
	RoutingKey   string
}

// ChannelMode controls whether the producer channel operates in confirmed or unsafe mode.
type ChannelMode int

const (
	// Confirmed enables publisher confirms. Broker acknowledges every message.
	// This is the safe default for CDC, saga, and any critical path.
	Confirmed ChannelMode = iota

	// Unsafe disables publisher confirms entirely. No delivery guarantee.
	// The broker does not track or acknowledge messages on this channel.
	// Use ONLY for high-throughput non-critical paths (metrics, logs, analytics).
	// Equivalent to Rust's unsafe {} — you are opting out of safety guarantees.
	Unsafe
)

// UnsafeOptions are options that can ONLY be used when ChannelMode is Unsafe.
// These options are meaningless and ignored in Confirmed mode.
type UnsafeOptions struct {
	FireAndForget bool
}

type ProducerChannelOptions struct {
	Mode          ChannelMode
	UnsafeOptions UnsafeOptions // only respected when Mode == Unsafe
}

type RabbitMqPublisherConfig struct {
	Persistent  bool
	Priority    uint8
	Expiration  string
	ContentType *string
	Headers     amqp.Table
}