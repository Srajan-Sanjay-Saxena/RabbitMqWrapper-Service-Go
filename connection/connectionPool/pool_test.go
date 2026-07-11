package connPool

import (
	"context"
	"testing"

	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
)

func TestNewConnectionPool(t *testing.T) {
	pool := NewConnectionPool(PoolConfig{
		ConnString:  "amqp://guest:guest@localhost:5672/",
		ConnSize:    5,
		ChanPerConn: 3,
		ConnOptions: singleConn.DefaultOptions(),
	})

	if pool.config.ConnSize != 5 {
		t.Errorf("expected ConnSize 5, got %d", pool.config.ConnSize)
	}
	if pool.config.ChanPerConn != 3 {
		t.Errorf("expected ChanPerConn 3, got %d", pool.config.ChanPerConn)
	}
	if pool.config.ConnString != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected conn string: %s", pool.config.ConnString)
	}
	if len(pool.connections) != 0 {
		t.Errorf("expected 0 connections before Connect, got %d", len(pool.connections))
	}
	if pool.log == nil {
		t.Error("expected logger to be initialized")
	}
}

func TestNewConnectionPoolDefaults(t *testing.T) {
	pool := NewConnectionPool(PoolConfig{
		ConnString:  "amqp://localhost/",
		ConnOptions: singleConn.DefaultOptions(),
	})

	if pool.config.ConnSize != 3 {
		t.Errorf("expected default ConnSize 3, got %d", pool.config.ConnSize)
	}
	if pool.config.ChanPerConn != 5 {
		t.Errorf("expected default ChanPerConn 5, got %d", pool.config.ChanPerConn)
	}
}

func TestGetChannelOnEmptyPool(t *testing.T) {
	pool := NewConnectionPool(PoolConfig{
		ConnString:  "amqp://localhost/",
		ConnSize:    3,
		ChanPerConn: 5,
		ConnOptions: singleConn.DefaultOptions(),
	})

	_, err := pool.GetChannel(context.Background(), nil)
	if err == nil {
		t.Error("expected error when getting channel from empty pool")
	}
	if err.Error() != "pool not initialized" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestConnectFailsWithBadURL(t *testing.T) {
	pool := NewConnectionPool(PoolConfig{
		ConnString:  "amqp://bad:bad@localhost:9999/",
		ConnSize:    2,
		ChanPerConn: 2,
		ConnOptions: singleConn.DefaultOptions(),
	})

	err := pool.Connect(context.Background())
	if err == nil {
		t.Error("expected error initializing pool with bad URL")
		pool.Shutdown()
	}
}

func TestShutdownWithNoConnections(t *testing.T) {
	pool := NewConnectionPool(PoolConfig{
		ConnString:  "amqp://localhost/",
		ConnSize:    3,
		ChanPerConn: 5,
		ConnOptions: singleConn.DefaultOptions(),
	})

	err := pool.Shutdown()
	if err != nil {
		t.Errorf("expected nil error on shutdown with no connections, got %v", err)
	}
}
