package integration_test

import (
	"context"
	"testing"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/exchange"
)

func TestCreateTopicExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.topic.exchange",
		Type:    exchange.Topic,
		Durable: true,
	})

	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}
}

func TestCreateDirectExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.direct.exchange",
		Type:    exchange.Direct,
		Durable: true,
	})

	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create direct exchange failed: %v", err)
	}
}

func TestCreateFanoutExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.fanout.exchange",
		Type:    exchange.Fanout,
		Durable: true,
	})

	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create fanout exchange failed: %v", err)
	}
}

func TestCreateQueueAndBind(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.exchange.q",
		Type:    exchange.Topic,
		Durable: true,
	})
	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	q, err := ex.CreateQueue(ctx, conn, exchange.RabbitQueueConfig{
		Name:       "test.queue.bind",
		BindingKey: "test.#",
		Durable:    true,
	})
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	if q.Name != "test.queue.bind" {
		t.Errorf("expected queue name 'test.queue.bind', got '%s'", q.Name)
	}
}

func TestCreateQuorumQueue(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.exchange.quorum",
		Type:    exchange.Topic,
		Durable: true,
	})
	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	q, err := ex.CreateQueue(ctx, conn, exchange.RabbitQueueConfig{
		Name:       "test.quorum.queue",
		BindingKey: "quorum.#",
		QueueType:  exchange.QuorumQueue,
		Durable:    true,
	})
	if err != nil {
		t.Fatalf("create quorum queue failed: %v", err)
	}
	if q.Name != "test.quorum.queue" {
		t.Errorf("expected 'test.quorum.queue', got '%s'", q.Name)
	}
}

func TestCreateExchangeIdempotent(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exchange.RabbitExchangeConfig{
		Name:    "test.idempotent.ex",
		Type:    exchange.Topic,
		Durable: true,
	})

	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("first declare failed: %v", err)
	}
	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("second declare (idempotent) failed: %v", err)
	}
}
