package helpers

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/channel"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/logger"
)

type IRabbitConnection interface {
	GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error)
	GetLogger() *logger.Logger
	Shutdown() error
}
