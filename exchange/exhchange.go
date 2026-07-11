package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"context"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/helpers"
)

type ExchangeTopic int

const (
	Topic ExchangeTopic = iota
	Direct
	Fanout
	Headers
)

func (et ExchangeTopic) String() string {
	switch et {
	case Topic:
		return "topic"
	case Direct:
		return "direct"
	case Fanout:
		return "fanout"
	case Headers:
		return "headers"
	default:
		return "unknown"
	}
}

type RabbitExchangeClass struct {
	config RabbitExchangeConfig
}

func NewRabbitExchange(cfg RabbitExchangeConfig) *RabbitExchangeClass {
	return &RabbitExchangeClass{config: cfg}
}

func (rbEx *RabbitExchangeClass) CreateExchange(ctx context.Context, conn helpers.IRabbitConnection) error {
	ch, err := conn.GetChannel(ctx, nil)
	if err != nil {
		return err
	}
	defer ch.Close()

	c := rbEx.config
	return ch.ExchangeDeclare(c.Name, c.Type.String(), c.Durable, c.AutoDelete, c.Internal, c.NoWait, nil)
}

func (rbEx *RabbitExchangeClass) CreateQueue(ctx context.Context, conn helpers.IRabbitConnection, cfg RabbitQueueConfig) (amqp.Queue, error) {
	ch, err := conn.GetChannel(ctx, nil)
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()

	args := cfg.Args
	if args == nil {
		args = amqp.Table{}
	}
	if cfg.QueueType != "" {
		args["x-queue-type"] = string(cfg.QueueType)
	}

	q, err := ch.QueueDeclare(cfg.Name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, args)
	if err != nil {
		return amqp.Queue{}, err
	}

	if err := ch.QueueBind(q.Name, cfg.BindingKey, rbEx.config.Name, cfg.NoWait, nil); err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}
