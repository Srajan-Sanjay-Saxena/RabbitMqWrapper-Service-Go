package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/exchange"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/producer"
)

func TestProducerPublishWithConfirm(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "prod.test.ex", "prod.test.q", "prod.test.#")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "prod.test.ex", RoutingKey: "prod.test.event"})
	if err := pub.GetChannel(ctx, conn, nil); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	err := pub.Publish(ctx, []byte(`{"event": "test"}`), producer.RabbitMqPublisherConfig{
		Persistent: true,
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
}

func TestProducerPublishWithTTL(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "ttl.ex", "ttl.q", "ttl.#")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "ttl.ex", RoutingKey: "ttl.msg"})
	if err := pub.GetChannel(ctx, conn, nil); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	err := pub.Publish(ctx, []byte(`{"otp": "1234"}`), producer.RabbitMqPublisherConfig{
		Persistent: true,
		Expiration: "5000",
	})
	if err != nil {
		t.Fatalf("publish with TTL failed: %v", err)
	}
}

func TestProducerPublishWithHeaders(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "headers.ex", "headers.q", "headers.#")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "headers.ex", RoutingKey: "headers.msg"})
	if err := pub.GetChannel(ctx, conn, nil); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	err := pub.Publish(ctx, []byte(`{"data": "with headers"}`), producer.RabbitMqPublisherConfig{
		Persistent: true,
		Headers: amqp.Table{
			"x-source":    "integration-test",
			"x-operation": "insert",
		},
	})
	if err != nil {
		t.Fatalf("publish with headers failed: %v", err)
	}
}

func TestProducerPublishUnroutableReturnsError(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create exchange but NO queue bound to this routing key
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{Name: "unroutable.ex", Type: exchange.Topic, Durable: true})
	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "unroutable.ex", RoutingKey: "no.queue.bound.here"})
	if err := pub.GetChannel(ctx, conn, nil); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	err := pub.Publish(ctx, []byte(`{"lost": true}`), producer.RabbitMqPublisherConfig{
		Persistent: true,
	})
	if err == nil {
		t.Fatal("expected error for unroutable message (mandatory=true)")
	}
}

func TestProducerMultiplePublishes(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "multi.ex", "multi.q", "multi.#")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "multi.ex", RoutingKey: "multi.msg"})
	if err := pub.GetChannel(ctx, conn, nil); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		err := pub.Publish(ctx, []byte(`{"seq": `+string(rune('0'+i))+`}`), producer.RabbitMqPublisherConfig{
			Persistent: true,
		})
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
}

func TestProducerContextCancelled(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "ctx.cancel.ex", "ctx.cancel.q", "ctx.cancel.#")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // ensure context is expired

	pub := producer.NewProducer(producer.ProducerConfig{ExchangeName: "ctx.cancel.ex", RoutingKey: "ctx.cancel.key"})
	pub.GetChannel(ctx, conn, nil)

	err := pub.Publish(ctx, []byte(`{"cancelled": true}`), producer.RabbitMqPublisherConfig{})
	if err == nil {
		t.Fatal("expected error on expired context")
	}
}
