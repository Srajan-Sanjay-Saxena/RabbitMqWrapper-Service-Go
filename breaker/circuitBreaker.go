package breaker

import (
	"context"
	"math/rand"
	"time"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/v2/logger"
)

type AfterProbeCb func()

type CircuitBreaker struct {
	failuresCount       int
	state               CircuitState
	currentResetTimeout time.Duration
	options             CircuitBreakerOptions
	log                 *logger.Logger
}

func NewCircuitBreaker(options CircuitBreakerOptions, log *logger.Logger) *CircuitBreaker {
	if log == nil {
		log = logger.New(logger.Production)
	}
	opts := CircuitBreakerOptions{
		threshold:        orDefault(options.threshold, 5),
		baseResetTimeout: orDefault(options.baseResetTimeout, 5*time.Second),
		maxResetTimeout:  orDefault(options.maxResetTimeout, 60*time.Second),
	}
	log.Info("circuit breaker initialized", "threshold", opts.threshold, "baseResetTimeout", opts.baseResetTimeout, "maxResetTimeout", opts.maxResetTimeout)
	return &CircuitBreaker{
		state:               Closed,
		currentResetTimeout: opts.baseResetTimeout,
		options:             opts,
		failuresCount:       0,
		log:                 log,
	}
}

func (cb *CircuitBreaker) IsOpen() bool {
	return cb.state == Open
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.log.Debug("recording success", "previousState", cb.state.String(), "failuresCount", cb.failuresCount)
	cb.failuresCount = 0
	cb.state = Closed
	cb.currentResetTimeout = cb.options.baseResetTimeout
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.failuresCount += 1
	if cb.state == HalfOpen || cb.failuresCount >= cb.options.threshold {
		cb.log.Warn("circuit opened", "failuresCount", cb.failuresCount, "threshold", cb.options.threshold)
		cb.state = Open
	} else {
		cb.log.Debug("failure recorded", "failuresCount", cb.failuresCount, "threshold", cb.options.threshold)
	}
}

func (cb *CircuitBreaker) GetBackoffDelay(maxInterval time.Duration) time.Duration {
	delay := time.Duration(1<<cb.failuresCount) * time.Second
	delay = min(delay, maxInterval)
	jittered := time.Duration(rand.Float64() * float64(delay))
	cb.log.Debug("backoff delay calculated", "delay", jittered, "failuresCount", cb.failuresCount)
	return jittered
}

func (cb *CircuitBreaker) Probe(ctx context.Context, afterWaitCb AfterProbeCb) error {
	cb.log.Info("probing circuit", "resetTimeout", cb.currentResetTimeout)
	select {
	case <-time.After(cb.currentResetTimeout):
		cb.state = HalfOpen
		cb.currentResetTimeout = min(cb.currentResetTimeout*2, cb.options.maxResetTimeout)
		cb.log.Info("circuit half-open, attempting recovery", "nextResetTimeout", cb.currentResetTimeout)
		afterWaitCb()
		return nil
	case <-ctx.Done():
		cb.log.Warn("probe cancelled", "reason", ctx.Err())
		return ctx.Err()
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.log.Info("circuit reset", "previousState", cb.state.String())
	cb.state = Closed
	cb.currentResetTimeout = cb.options.baseResetTimeout
	cb.failuresCount = 0
}
