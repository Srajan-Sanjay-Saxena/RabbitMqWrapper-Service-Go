package producer

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewProducer(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "test.exchange", RoutingKey: "test.routing.key"})

	if pub.config.ExchangeName != "test.exchange" {
		t.Errorf("expected 'test.exchange', got '%s'", pub.config.ExchangeName)
	}
	if pub.config.RoutingKey != "test.routing.key" {
		t.Errorf("expected 'test.routing.key', got '%s'", pub.config.RoutingKey)
	}
	if pub.channel != nil {
		t.Error("expected nil channel before GetChannel()")
	}
}

func TestBuildMessagePersistent(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Persistent: true,
	})

	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("expected Persistent delivery mode, got %d", msg.DeliveryMode)
	}
}

func TestBuildMessageTransient(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Persistent: false,
	})

	if msg.DeliveryMode != amqp.Transient {
		t.Errorf("expected Transient delivery mode, got %d", msg.DeliveryMode)
	}
}

func TestBuildMessageDefaultContentType(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	msg := pub.buildMessage(RabbitMqPublisherConfig{})

	if msg.ContentType != "application/json" {
		t.Errorf("expected 'application/json', got '%s'", msg.ContentType)
	}
}

func TestBuildMessageCustomContentType(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})
	ct := "text/plain"

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		ContentType: &ct,
	})

	if msg.ContentType != "text/plain" {
		t.Errorf("expected 'text/plain', got '%s'", msg.ContentType)
	}
}

func TestBuildMessagePriority(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Priority: 9,
	})

	if msg.Priority != 9 {
		t.Errorf("expected priority 9, got %d", msg.Priority)
	}
}

func TestBuildMessageExpiration(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Expiration: "60000",
	})

	if msg.Expiration != "60000" {
		t.Errorf("expected '60000', got '%s'", msg.Expiration)
	}
}

func TestBuildMessageHeaders(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	headers := amqp.Table{
		"x-source":  "cdc",
		"x-version": "1.0",
	}

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Headers: headers,
	})

	if msg.Headers["x-source"] != "cdc" {
		t.Error("expected x-source header")
	}
	if msg.Headers["x-version"] != "1.0" {
		t.Error("expected x-version header")
	}
}

func TestPublishFailsWithoutChannel(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})

	err := pub.Publish(context.Background(), []byte("test"), RabbitMqPublisherConfig{})
	if err == nil {
		t.Error("expected error when publishing without channel")
	}
	if err.Error() != "channel not initialized or closed, call GetChannel" {
		t.Errorf("unexpected error: %s", err.Error())
	}
}


func TestBuildMessageFullOptions(t *testing.T) {
	pub := NewProducer(ProducerConfig{ExchangeName: "ex", RoutingKey: "rk"})
	ct := "application/xml"

	msg := pub.buildMessage(RabbitMqPublisherConfig{
		Persistent:  true,
		Priority:    5,
		Expiration:  "30000",
		ContentType: &ct,
		Headers:     amqp.Table{"x-retry": int32(3)},
	})

	if msg.DeliveryMode != amqp.Persistent {
		t.Error("expected persistent")
	}
	if msg.Priority != 5 {
		t.Error("expected priority 5")
	}
	if msg.Expiration != "30000" {
		t.Error("expected expiration 30000")
	}
	if msg.ContentType != "application/xml" {
		t.Error("expected application/xml")
	}
	if msg.Headers["x-retry"] != int32(3) {
		t.Error("expected x-retry header")
	}
}
