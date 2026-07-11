package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/breaker"
	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
)

func TestConnectionConnect(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	if conn.Connection == nil {
		t.Fatal("expected non-nil connection")
	}
	if conn.Connection.IsClosed() {
		t.Fatal("expected open connection")
	}
}

func TestConnectionShutdown(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)

	err := conn.Shutdown()
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	if !conn.Connection.IsClosed() {
		t.Fatal("expected connection to be closed after shutdown")
	}
}

func TestConnectionGetChannel(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := setupConn(t, connStr)
	defer conn.Shutdown()

	ch, err := conn.GetChannel(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to get channel: %v", err)
	}
	defer ch.Close()
}

func TestConnectionCustomOptions(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	opts := singleConn.ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 10 * time.Second},
		ReconnectInterval:    2 * time.Second,
		MaxReconnectAttempts: 3,
	}

	conn := singleConn.NewRabbitMqSingleConnectionHandler(singleConn.SingleConnectionConfig{
		ConnString: connStr,
		Options:    opts,
	})
	conn.AddBreaker(breaker.CircuitBreakerOptions{})
	if err := conn.Connect(context.Background()); err != nil {
		t.Fatalf("failed to connect with custom opts: %v", err)
	}
	defer conn.Shutdown()

	if conn.Connection.IsClosed() {
		t.Fatal("expected open connection")
	}
}
