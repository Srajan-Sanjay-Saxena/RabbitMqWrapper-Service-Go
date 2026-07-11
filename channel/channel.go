package channel

import (
	"context"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OnChannelClose func(conn *amqp.Connection)

type ChannelHandler struct {
	logger  *logger.Logger
}

func NewChannelHandler(log *logger.Logger) *ChannelHandler {
	if log == nil {
		log = logger.New(logger.Production)
	}
	return &ChannelHandler{
		logger:  log,
	}
}

func (ch *ChannelHandler) GetChannel(ctx context.Context, conn *amqp.Connection , onClose OnChannelClose) (*amqp.Channel, error) {
	if conn == nil || conn.IsClosed() {
		return nil, amqp.ErrClosed
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	go ch.HandleChannelClose(ctx, channel, conn, onClose)

	return channel, nil
}

func (ch *ChannelHandler) HandleChannelClose(ctx context.Context, channel *amqp.Channel, conn *amqp.Connection , onClose OnChannelClose) {
	closeCh := channel.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closeCh:
		if err != nil {
			ch.logger.Warn("channel closed unexpectedly", "error", err)
		}
		if onClose != nil {
			onClose(conn)
		}
	case <-ctx.Done():
		return
	}
}
