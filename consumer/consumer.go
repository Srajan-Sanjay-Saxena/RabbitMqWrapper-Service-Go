package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/helpers"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/logger"
)

type MessageHandler func(ctx context.Context, msg amqp.Delivery) error

type ConsumerConfig struct {
	QueueName string
	Prefetch  int
	AutoAck   bool
	Handler   MessageHandler
}

type RabbitMqConsumer struct {
	config      ConsumerConfig
	channel     *amqp.Channel
	consumerTag string
	conn        helpers.IRabbitConnection
	ctx         context.Context
	log         *logger.Logger
	wg          sync.WaitGroup
}

func NewConsumer(cfg ConsumerConfig) *RabbitMqConsumer {
	return &RabbitMqConsumer{config: cfg}
}

func (c *RabbitMqConsumer) GetChannel(ctx context.Context, conn helpers.IRabbitConnection) error {
	c.conn = conn
	c.ctx = ctx
	c.log = conn.GetLogger()

	return c.acquireChannel()
}

func (c *RabbitMqConsumer) acquireChannel() error {
	ch, err := c.conn.GetChannel(c.ctx, func(_ *amqp.Connection) {
		c.wg.Wait()
		c.channel = nil

		go c.reacquire()
	})
	if err != nil {
		return err
	}

	if err := ch.Qos(c.config.Prefetch, 0, false); err != nil {
		ch.Close()
		return err
	}

	c.channel = ch
	return nil
}

func (c *RabbitMqConsumer) reacquire() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if err := c.acquireChannel(); err != nil {
			c.log.Error("consumer channel reacquire failed", "error", err)
			select {
			case <-time.After(2 * time.Second):
			case <-c.ctx.Done():
				return
			}
			continue
		}

		c.log.Info("consumer channel reacquired, restarting consume")
		if err := c.Consume(c.ctx); err != nil {
			c.log.Error("consumer restart failed", "error", err)
		}
		return
	}
}

func (c *RabbitMqConsumer) Consume(ctx context.Context) error {
	if c.channel == nil {
		return errors.New("channel not initialized, call GetChannel first")
	}

	msgs, err := c.channel.Consume(c.config.QueueName, c.consumerTag, c.config.AutoAck, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					c.log.Warn("consumer channel closed")
					return
				}
				c.wg.Add(1)
				go func(m amqp.Delivery) {
					defer c.wg.Done()
					if err := c.config.Handler(ctx, m); err != nil {
						c.log.Error("handler error", "error", err)
						m.Nack(false, true)
					} else {
						m.Ack(false)
					}
				}(msg)
			case <-ctx.Done():
				c.log.Info("consumer stopping, waiting for in-flight messages")
				c.channel.Cancel(c.consumerTag, false)
				c.wg.Wait()
				c.channel.Close()
				return
			}
		}
	}()

	return nil
}
