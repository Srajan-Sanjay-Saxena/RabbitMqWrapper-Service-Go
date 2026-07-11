package integration_test

import (
	"context"
	"testing"

	connPool "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/connection/connectionPool"
	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/connection/singleConnection"
)

func TestPoolConnect(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connPool.NewConnectionPool(connPool.PoolConfig{
		ConnString:  connStr,
		ConnSize:    3,
		ChanPerConn: 5,
		ConnOptions: singleConn.DefaultOptions(),
	})
	if err := pool.Connect(context.Background()); err != nil {
		t.Fatalf("pool connect failed: %v", err)
	}
	defer pool.Shutdown()
}

func TestPoolGetChannel(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connPool.NewConnectionPool(connPool.PoolConfig{
		ConnString:  connStr,
		ConnSize:    3,
		ChanPerConn: 5,
		ConnOptions: singleConn.DefaultOptions(),
	})
	if err := pool.Connect(context.Background()); err != nil {
		t.Fatalf("pool connect failed: %v", err)
	}
	defer pool.Shutdown()

	ch, err := pool.GetChannel(context.Background(), nil)
	if err != nil {
		t.Fatalf("get channel failed: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

func TestPoolExhaustion(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connPool.NewConnectionPool(connPool.PoolConfig{
		ConnString:  connStr,
		ConnSize:    1,
		ChanPerConn: 2,
		ConnOptions: singleConn.DefaultOptions(),
	})
	if err := pool.Connect(context.Background()); err != nil {
		t.Fatalf("pool connect failed: %v", err)
	}
	defer pool.Shutdown()

	// Acquire all channels
	ch1, err := pool.GetChannel(context.Background(), nil)
	if err != nil {
		t.Fatalf("get channel 1 failed: %v", err)
	}
	ch2, err := pool.GetChannel(context.Background(), nil)
	if err != nil {
		t.Fatalf("get channel 2 failed: %v", err)
	}

	// Pool should be exhausted
	_, err = pool.GetChannel(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when pool is exhausted")
	}

	_ = ch1
	_ = ch2
}

func TestPoolShutdown(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connPool.NewConnectionPool(connPool.PoolConfig{
		ConnString:  connStr,
		ConnSize:    2,
		ChanPerConn: 3,
		ConnOptions: singleConn.DefaultOptions(),
	})
	if err := pool.Connect(context.Background()); err != nil {
		t.Fatalf("pool connect failed: %v", err)
	}

	err := pool.Shutdown()
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}
