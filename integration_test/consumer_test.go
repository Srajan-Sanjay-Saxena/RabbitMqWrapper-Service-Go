package integration_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/consumer"
)

func TestConsumerReceivesMessages(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "cons.test.ex", "cons.test.q", "cons.test.#")
	publishMessages(t, conn, "cons.test.ex", "cons.test.event", 5)

	var received atomic.Int32
	handler := func(ctx context.Context, msg amqp.Delivery) error {
		received.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	cons := consumer.NewConsumer(consumer.ConsumerConfig{QueueName: "cons.test.q", Prefetch: 10, Handler: handler})
	if err := cons.GetChannel(ctx, conn); err != nil {
		t.Fatalf("consumer get channel failed: %v", err)
	}

	if err := cons.Consume(ctx); err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	time.Sleep(1 * time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 5 {
		t.Errorf("expected 5 messages, got %d", received.Load())
	}
}

func TestConsumerHandlerErrorNacks(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "nack.ex", "nack.q", "nack.#")
	publishMessages(t, conn, "nack.ex", "nack.event", 1)

	var attempts atomic.Int32
	handler := func(ctx context.Context, msg amqp.Delivery) error {
		count := attempts.Add(1)
		if count <= 2 {
			return fmt.Errorf("simulated failure")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := consumer.NewConsumer(consumer.ConsumerConfig{QueueName: "nack.q", Prefetch: 1, Handler: handler})
	if err := cons.GetChannel(ctx, conn); err != nil {
		t.Fatalf("consumer get channel failed: %v", err)
	}

	cons.Consume(ctx)
	time.Sleep(2 * time.Second)
	cancel()

	if attempts.Load() < 2 {
		t.Errorf("expected at least 2 attempts (nack+requeue), got %d", attempts.Load())
	}
}

func TestConsumerGracefulShutdown(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "graceful.ex", "graceful.q", "graceful.#")
	publishMessages(t, conn, "graceful.ex", "graceful.event", 3)

	var completed atomic.Int32
	handler := func(ctx context.Context, msg amqp.Delivery) error {
		time.Sleep(200 * time.Millisecond)
		completed.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	cons := consumer.NewConsumer(consumer.ConsumerConfig{QueueName: "graceful.q", Prefetch: 10, Handler: handler})
	if err := cons.GetChannel(ctx, conn); err != nil {
		t.Fatalf("consumer get channel failed: %v", err)
	}

	cons.Consume(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(500 * time.Millisecond) // give time for wg.Wait to complete

	if completed.Load() == 0 {
		t.Error("expected at least some messages to complete before context cancel returned")
	}
}

func TestConsumerContextCancellation(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "ctxcancel.ex", "ctxcancel.q", "ctxcancel.#")

	handler := func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	cons := consumer.NewConsumer(consumer.ConsumerConfig{QueueName: "ctxcancel.q", Prefetch: 10, Handler: handler})
	if err := cons.GetChannel(ctx, conn); err != nil {
		t.Fatalf("consumer get channel failed: %v", err)
	}

	cons.Consume(ctx)
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestConsumerWithPrefetch(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "prefetch.ex", "prefetch.q", "prefetch.#")
	publishMessages(t, conn, "prefetch.ex", "prefetch.event", 20)

	var received atomic.Int32
	handler := func(ctx context.Context, msg amqp.Delivery) error {
		time.Sleep(50 * time.Millisecond)
		received.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	cons := consumer.NewConsumer(consumer.ConsumerConfig{QueueName: "prefetch.q", Prefetch: 5, Handler: handler})
	if err := cons.GetChannel(ctx, conn); err != nil {
		t.Fatalf("consumer get channel failed: %v", err)
	}

	cons.Consume(ctx)
	time.Sleep(3 * time.Second)
	cancel()
	time.Sleep(200 * time.Millisecond)

	if received.Load() != 20 {
		t.Errorf("expected 20 messages processed, got %d", received.Load())
	}
}
