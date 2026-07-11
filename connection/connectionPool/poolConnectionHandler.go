package connPool

import (
	"context"
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/breaker"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/channel"
	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/logger"
)

type PoolConfig struct {
	ConnString  string
	ConnSize    int
	ChanPerConn int
	ConnOptions singleConn.ConnectionOptions
	Logger      *logger.Logger
}

type RabbitMqConnectionPoolHandler struct {
	connections    []*singleConn.RabbitMqSingleConnectionHandler
	chanPool       map[*singleConn.RabbitMqSingleConnectionHandler]chan *amqp.Channel
	channelHandler *channel.ChannelHandler
	config         PoolConfig
	connIdx        int
	log            *logger.Logger
	mu             sync.Mutex
}

func NewConnectionPool(cfg PoolConfig) *RabbitMqConnectionPoolHandler {
	if cfg.Logger == nil {
		cfg.Logger = logger.New(logger.Production)
	}
	if cfg.ConnSize == 0 {
		cfg.ConnSize = 3
	}
	if cfg.ChanPerConn == 0 {
		cfg.ChanPerConn = 5
	}

	p := &RabbitMqConnectionPoolHandler{
		config:   cfg,
		log:      cfg.Logger,
		chanPool: make(map[*singleConn.RabbitMqSingleConnectionHandler]chan *amqp.Channel),
	}

	p.channelHandler = channel.NewChannelHandler(cfg.Logger)

	return p
}

func (p *RabbitMqConnectionPoolHandler) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.config.ConnSize; i++ {
		conn := singleConn.NewRabbitMqSingleConnectionHandler(singleConn.SingleConnectionConfig{
			ConnString: p.config.ConnString,
			Options:    p.config.ConnOptions,
			Logger:     p.log,
		})
		conn.AddBreaker(breaker.CircuitBreakerOptions{})
		if err := conn.Connect(ctx); err != nil {
			return err
		}
		p.connections = append(p.connections, conn)

		// Pre-warm channel buffer
		buf := make(chan *amqp.Channel, p.config.ChanPerConn)
		for j := 0; j < p.config.ChanPerConn; j++ {
			ch, err := p.channelHandler.GetChannel(ctx, conn.Connection, p.replaceDeadChannel)
			if err != nil {
				return err
			}
			buf <- ch
		}
		p.chanPool[conn] = buf
	}

	p.log.Info("connection pool initialized", "connections", p.config.ConnSize, "channelsPerConn", p.config.ChanPerConn)
	return nil
}

func (p *RabbitMqConnectionPoolHandler) GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error) {
	p.mu.Lock()
	if len(p.connections) == 0 {
		p.mu.Unlock()
		return nil, errors.New("pool not initialized")
	}
	startIdx := p.connIdx
	p.connIdx = (p.connIdx + 1) % len(p.connections)
	p.mu.Unlock()

	for i := 0; i < len(p.connections); i++ {
		conn := p.connections[(startIdx+i)%len(p.connections)]

		if conn.Connection == nil || conn.Connection.IsClosed() {
			continue
		}

		select {
		case ch := <-p.chanPool[conn]:
			// If caller provided onClose, watch for this specific channel's death
			if onClose != nil {
				go p.channelHandler.HandleChannelClose(ctx, ch, conn.Connection, onClose)
			}
			return ch, nil
		default:
			continue
		}
	}

	return nil, errors.New("all channels acquired — pool exhausted")
}

func (p *RabbitMqConnectionPoolHandler) ReleaseChannel(targetConn *amqp.Connection, ch *amqp.Channel) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		if conn.Connection != targetConn {
			continue
		}
		select {
		case p.chanPool[conn] <- ch:
			return
		default:
			p.log.Warn("buffer full, closing channel")
			ch.Close()
			return
		}
	}

	p.log.Warn("connection not found in pool, closing orphaned channel")
	ch.Close()
}

func (p *RabbitMqConnectionPoolHandler) replaceDeadChannel(conn *amqp.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the connection handler that owns this amqp.Connection
	for connHandler, buf := range p.chanPool {
		if connHandler.Connection != conn {
			continue
		}

		if conn.IsClosed() {
			p.log.Warn("connection closed, skipping channel replacement")
			return
		}

		newCh, err := conn.Channel()
		if err != nil {
			p.log.Error("failed to replace dead channel", "error", err)
			return
		}

		// Watch the new channel
		go p.channelHandler.HandleChannelClose(context.Background(), newCh, conn ,p.replaceDeadChannel)

		// Put replacement into buffer (non-blocking)
		select {
		case buf <- newCh:
			p.log.Info("replaced dead channel")
		default:
			// Buffer full — shouldn't happen since one died, but close to be safe
			newCh.Close()
		}
		return
	}
}

func (p *RabbitMqConnectionPoolHandler) GetLogger() *logger.Logger {
	return p.log
}

func (p *RabbitMqConnectionPoolHandler) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Drain and close all channels
	for _, buf := range p.chanPool {
		close(buf)
		for ch := range buf {
			if ch != nil {
				ch.Close()
			}
		}
	}

	// Close all connections
	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Shutdown(); err != nil {
			lastErr = err
		}
	}

	p.log.Info("connection pool shut down")
	return lastErr
}
