package singleConn

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/breaker"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/channel"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/logger"
)

type RabbitMqSingleConnectionHandler struct {
	Connection              *amqp.Connection
	rabbitConnString        string
	breaker                *breaker.CircuitBreaker
	log                    *logger.Logger
	channelHandler         *channel.ChannelHandler
	options                ConnectionOptions
	shutDownInitiated      bool
	reconnectAttempts       int
	onReconnectCallbacks   []func() error
	onPermanentFailureCb   func(error) error
	mu                     sync.RWMutex
}

func DefaultOptions() ConnectionOptions {
	return ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 60 * time.Second},
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
	}
}

type SingleConnectionConfig struct {
	ConnString string
	Options    ConnectionOptions
	Logger     *logger.Logger
}

func NewRabbitMqSingleConnectionHandler(cfg SingleConnectionConfig) *RabbitMqSingleConnectionHandler {
	log := cfg.Logger
	if log == nil {
		log = logger.New(logger.Production)
	}
	opts := cfg.Options
	if opts.MaxReconnectAttempts < 0 {
		opts.MaxReconnectAttempts = 10
	}
	if opts.ReconnectInterval <= 0 {
		opts.ReconnectInterval = 5 * time.Second
	}
	if opts.AmqpConfig.Heartbeat <= 0 {
		opts.AmqpConfig.Heartbeat = 60 * time.Second
	}
	return &RabbitMqSingleConnectionHandler{
		rabbitConnString: cfg.ConnString,
		options:         opts,
		log:             log,
		channelHandler:  channel.NewChannelHandler(log),
	}
}

func (rabbit *RabbitMqSingleConnectionHandler) AddBreaker(opts breaker.CircuitBreakerOptions) {
	rabbit.breaker = breaker.NewCircuitBreaker(opts, rabbit.log)
}

func (rabbit *RabbitMqSingleConnectionHandler) Connect(ctx context.Context) error {
	rabbit.mu.Lock()
	defer rabbit.mu.Unlock()

	conn, err := amqp.DialConfig(rabbit.rabbitConnString, rabbit.options.AmqpConfig)
	if err != nil {
		return err
	}

	rabbit.Connection = conn

	go rabbit.handleDisconnect(ctx)

	return nil
}

func (rabbit *RabbitMqSingleConnectionHandler) handleDisconnect(ctx context.Context) {
	closeCh := rabbit.Connection.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case err := <-closeCh:
		if err != nil {
			rabbit.log.Error("connection error", "error", err)
		}
		rabbit.log.Warn("connection closed")

		rabbit.mu.Lock()
		hasShutdownInitiated := rabbit.shutDownInitiated
		rabbit.mu.Unlock()

		if !hasShutdownInitiated {
			rabbit.breaker.RecordFailure()
			rabbit.reconnect(ctx)
		}
	case <-ctx.Done():
		return
	}
}

func (rabbit *RabbitMqSingleConnectionHandler) reconnect(ctx context.Context) {
	rabbit.mu.Lock()
	rabbit.reconnectAttempts++
	attempt := rabbit.reconnectAttempts
	maxAttempts := rabbit.options.MaxReconnectAttempts
	rabbit.mu.Unlock()

	if attempt > maxAttempts {
		err := fmt.Errorf("max reconnect attempts (%d) exhausted", maxAttempts)
		rabbit.log.Error("permanent failure", "error", err)
		if rabbit.onPermanentFailureCb != nil {
			if cbErr := rabbit.onPermanentFailureCb(err); cbErr != nil {
				rabbit.log.Error("permanent failure callback error", "error", cbErr)
			}
		}
		return
	}

	if rabbit.breaker.IsOpen() {
		rabbit.log.Warn("circuit open — pausing reconnect", "attempt", attempt)
		rabbit.breaker.Probe(ctx, func() {
			rabbit.reconnect(ctx)
		})
		return
	}

	delay := rabbit.breaker.GetBackoffDelay(30 * time.Second)
	rabbit.log.Info("attempting reconnect", "attempt", attempt, "delay", delay)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return
	}

	if err := rabbit.Connect(ctx); err != nil {
		rabbit.log.Error("reconnect failed", "error", err, "attempt", attempt)
		rabbit.breaker.RecordFailure()
		rabbit.reconnect(ctx)
		return
	}

	rabbit.mu.Lock()
	rabbit.reconnectAttempts = 0
	rabbit.mu.Unlock()

	rabbit.breaker.RecordSuccess()
	rabbit.log.Info("reconnected successfully")

	for _, cb := range rabbit.onReconnectCallbacks {
		if err := cb(); err != nil {
			rabbit.log.Error("reconnect callback error", "error", err)
		}
	}
}

func (rabbit *RabbitMqSingleConnectionHandler) GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error) {
	rabbit.mu.RLock()
	conn := rabbit.Connection
	rabbit.mu.RUnlock()
	return rabbit.channelHandler.GetChannel(ctx, conn, onClose)
}

func (rabbit *RabbitMqSingleConnectionHandler) OnReconnect(cb func() error) {
	rabbit.mu.Lock()
	defer rabbit.mu.Unlock()
	rabbit.onReconnectCallbacks = append(rabbit.onReconnectCallbacks, cb)
}

func (rabbit *RabbitMqSingleConnectionHandler) OnPermanentFailure(cb func(error) error) {
	rabbit.mu.Lock()
	defer rabbit.mu.Unlock()
	rabbit.onPermanentFailureCb = cb
}

func (rabbit *RabbitMqSingleConnectionHandler) GetLogger() *logger.Logger {
	return rabbit.log
}

func (rabbit *RabbitMqSingleConnectionHandler) Shutdown() error {
	rabbit.mu.Lock()
	rabbit.shutDownInitiated = true
	rabbit.mu.Unlock()

	if rabbit.Connection != nil {
		return rabbit.Connection.Close()
	}
	return nil
}
