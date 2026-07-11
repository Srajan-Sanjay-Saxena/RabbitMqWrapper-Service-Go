package singleConn

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.AmqpConfig.Heartbeat != 60*time.Second {
		t.Errorf("expected heartbeat 60s, got %v", opts.AmqpConfig.Heartbeat)
	}
	if opts.ReconnectInterval != 5*time.Second {
		t.Errorf("expected reconnect interval 5s, got %v", opts.ReconnectInterval)
	}
	if opts.MaxReconnectAttempts != 10 {
		t.Errorf("expected max reconnect attempts 10, got %d", opts.MaxReconnectAttempts)
	}
}

func TestNewRabbitMqSingleConnectionHandler(t *testing.T) {
	opts := ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 30 * time.Second},
		ReconnectInterval:    2 * time.Second,
		MaxReconnectAttempts: 5,
	}

	conn := NewRabbitMqSingleConnectionHandler(SingleConnectionConfig{
		ConnString: "amqp://guest:guest@localhost:5672/",
		Options:    opts,
	})

	if conn.rabbitConnString != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected conn string: %s", conn.rabbitConnString)
	}
	if conn.options.MaxReconnectAttempts != 5 {
		t.Errorf("expected 5, got %d", conn.options.MaxReconnectAttempts)
	}
	if conn.Connection != nil {
		t.Error("expected nil connection before Connect()")
	}
	if conn.shutDownInitiated {
		t.Error("expected shutDownInitiated to be false")
	}
	if conn.log == nil {
		t.Error("expected logger to be initialized")
	}
}

func TestConnectFailsWithBadURL(t *testing.T) {
	conn := NewRabbitMqSingleConnectionHandler(SingleConnectionConfig{
		ConnString: "amqp://invalid:invalid@localhost:9999/",
		Options:    DefaultOptions(),
	})

	err := conn.Connect(context.Background())
	if err == nil {
		t.Error("expected error connecting to invalid URL")
		conn.Shutdown()
	}
}

func TestShutdownWithNilConnection(t *testing.T) {
	conn := NewRabbitMqSingleConnectionHandler(SingleConnectionConfig{
		ConnString: "amqp://guest:guest@localhost:5672/",
		Options:    DefaultOptions(),
	})

	err := conn.Shutdown()
	if err != nil {
		t.Errorf("expected nil error on shutdown with nil connection, got %v", err)
	}
	if !conn.shutDownInitiated {
		t.Error("expected shutDownInitiated to be true after Shutdown()")
	}
}

func TestShutdownSetsFlag(t *testing.T) {
	conn := NewRabbitMqSingleConnectionHandler(SingleConnectionConfig{
		ConnString: "amqp://guest:guest@localhost:5672/",
		Options:    DefaultOptions(),
	})

	conn.Shutdown()

	if !conn.shutDownInitiated {
		t.Error("expected shutDownInitiated = true")
	}
}
