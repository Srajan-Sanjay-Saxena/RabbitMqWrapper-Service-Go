package consumer

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewConsumerFields(t *testing.T) {
	cons := NewConsumer(ConsumerConfig{QueueName: "test.queue", Prefetch: 15, Handler: func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	}})

	if cons.config.QueueName != "test.queue" {
		t.Errorf("expected 'test.queue', got '%s'", cons.config.QueueName)
	}
	if cons.config.Prefetch != 15 {
		t.Errorf("expected prefetch 15, got %d", cons.config.Prefetch)
	}
	if cons.channel != nil {
		t.Error("expected nil channel before GetChannel()")
	}
	if cons.config.AutoAck != false {
		t.Error("expected autoAck false by default")
	}
	if cons.config.Handler == nil {
		t.Error("expected handler to be set")
	}
}

func TestConsumeFailsWithoutChannel(t *testing.T) {
	cons := NewConsumer(ConsumerConfig{QueueName: "test.queue", Prefetch: 10, Handler: func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	}})

	err := cons.Consume(context.Background())
	if err == nil {
		t.Error("expected error when consuming without channel")
	}
	if err.Error() != "channel not initialized, call GetChannel first" {
		t.Errorf("unexpected error: %s", err.Error())
	}
}

func TestNewConsumerWithDifferentPrefetch(t *testing.T) {
	tests := []struct {
		prefetch int
	}{
		{1},
		{10},
		{50},
		{100},
	}

	for _, tt := range tests {
		cons := NewConsumer(ConsumerConfig{QueueName: "q", Prefetch: tt.prefetch, Handler: func(ctx context.Context, msg amqp.Delivery) error {
			return nil
		}})
		if cons.config.Prefetch != tt.prefetch {
			t.Errorf("expected prefetch %d, got %d", tt.prefetch, cons.config.Prefetch)
		}
	}
}

func TestNewConsumerHandlerIsSet(t *testing.T) {
	cons := NewConsumer(ConsumerConfig{QueueName: "q", Prefetch: 10, Handler: func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	}})

	if cons.config.Handler == nil {
		t.Fatal("handler should not be nil")
	}
}

func TestNewConsumerConsumerTagEmpty(t *testing.T) {
	cons := NewConsumer(ConsumerConfig{QueueName: "q", Prefetch: 10, Handler: func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	}})

	if cons.consumerTag != "" {
		t.Errorf("expected empty consumer tag, got '%s'", cons.consumerTag)
	}
}
