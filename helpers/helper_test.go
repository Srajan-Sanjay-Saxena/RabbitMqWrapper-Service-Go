package helpers

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/channel"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/logger"
)

func TestIRabbitConnectionInterface(t *testing.T) {
	var _ IRabbitConnection = (*mockConn)(nil)
}

type mockConn struct{}

func (m *mockConn) GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error) {
	return nil, nil
}
func (m *mockConn) GetLogger() *logger.Logger { return logger.New(logger.Production) }
func (m *mockConn) Shutdown() error            { return nil }
