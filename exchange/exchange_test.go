package exchange

import (
	"testing"
)

func TestExchangeTopicString(t *testing.T) {
	tests := []struct {
		input    ExchangeTopic
		expected string
	}{
		{Topic, "topic"},
		{Direct, "direct"},
		{Fanout, "fanout"},
		{Headers, "headers"},
		{ExchangeTopic(99), "unknown"},
	}

	for _, tt := range tests {
		result := tt.input.String()
		if result != tt.expected {
			t.Errorf("ExchangeTopic(%d).String() = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestNewRabbitExchange(t *testing.T) {
	ex := NewRabbitExchange(RabbitExchangeConfig{
		Name:    "test.exchange",
		Type:    Topic,
		Durable: true,
	})

	if ex.config.Name != "test.exchange" {
		t.Errorf("expected 'test.exchange', got '%s'", ex.config.Name)
	}
	if ex.config.Type != Topic {
		t.Errorf("expected Topic, got %v", ex.config.Type)
	}
	if !ex.config.Durable {
		t.Error("expected Durable true")
	}
}

func TestNewRabbitExchangeDirectType(t *testing.T) {
	ex := NewRabbitExchange(RabbitExchangeConfig{Name: "direct.ex", Type: Direct, Durable: true})

	if ex.config.Type != Direct {
		t.Errorf("expected Direct, got %v", ex.config.Type)
	}
	if ex.config.Type.String() != "direct" {
		t.Errorf("expected 'direct', got '%s'", ex.config.Type.String())
	}
}

func TestNewRabbitExchangeFanoutType(t *testing.T) {
	ex := NewRabbitExchange(RabbitExchangeConfig{Name: "fanout.ex", Type: Fanout})

	if ex.config.Type != Fanout {
		t.Errorf("expected Fanout, got %v", ex.config.Type)
	}
	if ex.config.Type.String() != "fanout" {
		t.Errorf("expected 'fanout', got '%s'", ex.config.Type.String())
	}
}

func TestNewRabbitExchangeHeadersType(t *testing.T) {
	ex := NewRabbitExchange(RabbitExchangeConfig{Name: "headers.ex", Type: Headers, Internal: true})

	if ex.config.Type != Headers {
		t.Errorf("expected Headers, got %v", ex.config.Type)
	}
	if !ex.config.Internal {
		t.Error("expected Internal true")
	}
}
