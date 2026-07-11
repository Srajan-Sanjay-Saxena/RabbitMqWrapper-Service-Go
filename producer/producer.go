package producer

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/helpers"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/logger"
)

type RabbitMqProducer struct {
	config    ProducerConfig
	channel   *amqp.Channel
	confirmCh chan amqp.Confirmation
	returnCh  chan amqp.Return
	conn      helpers.IRabbitConnection
	opts      *ProducerChannelOptions
	ctx       context.Context
	log       *logger.Logger
}

func NewProducer(cfg ProducerConfig) *RabbitMqProducer {
	return &RabbitMqProducer{config: cfg}
}

func (rProd *RabbitMqProducer) GetChannel(ctx context.Context, conn helpers.IRabbitConnection, opts *ProducerChannelOptions) error {
	rProd.conn = conn
	rProd.ctx = ctx
	rProd.opts = opts
	rProd.log = conn.GetLogger()

	return rProd.acquireChannel()
}

func (rProd *RabbitMqProducer) acquireChannel() error {
	ch, err := rProd.conn.GetChannel(rProd.ctx, func(_ *amqp.Connection) {
		rProd.channel = nil
		rProd.confirmCh = nil
		rProd.returnCh = nil

		go rProd.reacquire()
	})
	if err != nil {
		return err
	}

	mode := Confirmed
	fireAndForget := false
	if rProd.opts != nil {
		mode = rProd.opts.Mode
		if mode == Unsafe {
			fireAndForget = rProd.opts.UnsafeOptions.FireAndForget
		}
	}

	if mode == Confirmed || (mode == Unsafe && !fireAndForget) {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return err
		}
		rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))
	}

	rProd.channel = ch
	return nil
}

func (rProd *RabbitMqProducer) reacquire() {
	for {
		if err := rProd.acquireChannel(); err != nil {
			rProd.log.Error("producer channel reacquire failed", "error", err)
			select {
			case <-time.After(2 * time.Second):
			case <-rProd.ctx.Done():
				return
			}
			continue
		}
		rProd.log.Info("producer channel reacquired")
		return
	}
}

func (rProd *RabbitMqProducer) Publish(ctx context.Context, body []byte, cfg RabbitMqPublisherConfig) error {
	if rProd.channel == nil {
		return errors.New("channel not initialized or closed, call GetChannel")
	}

	mode := Confirmed
	fireAndForget := false
	if rProd.opts != nil {
		mode = rProd.opts.Mode
		if mode == Unsafe {
			fireAndForget = rProd.opts.UnsafeOptions.FireAndForget
		}
	}

	msg := rProd.buildMessage(cfg)
	msg.Body = body

	err := rProd.channel.PublishWithContext(ctx, rProd.config.ExchangeName, rProd.config.RoutingKey, mode == Confirmed, false, msg)
	if err != nil {
		return err
	}

	if mode == Unsafe && fireAndForget {
		return nil
	}

	select {
	case confirm := <-rProd.confirmCh:
		if !confirm.Ack {
			return errors.New("broker nacked the message")
		}
		return nil
	case ret := <-rProd.returnCh:
		return errors.New("message returned: " + ret.ReplyText)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rProd *RabbitMqProducer) buildMessage(cfg RabbitMqPublisherConfig) amqp.Publishing {
	deliveryMode := amqp.Transient
	if cfg.Persistent {
		deliveryMode = amqp.Persistent
	}

	contentType := "application/json"
	if cfg.ContentType != nil {
		contentType = *cfg.ContentType
	}

	return amqp.Publishing{
		DeliveryMode: deliveryMode,
		Priority:     cfg.Priority,
		Expiration:   cfg.Expiration,
		ContentType:  contentType,
		Headers:      cfg.Headers,
	}
}
