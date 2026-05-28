package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-zeromq/zmq4"
)

// frameSource yields ZMQ multipart frames. It is an interface so the subscribe
// loop can be tested without a real socket.
type frameSource interface {
	Recv() ([][]byte, error)
	Close() error
}

// dialFunc opens a frameSource (or fails). Overridable in tests.
type dialFunc func(ctx context.Context) (frameSource, error)

// Subscriber reads a vLLM KV-cache event stream from a ZMQ PUB endpoint, decodes
// each batch, and emits it. It reconnects with backoff and never returns except
// on context cancellation (fail-soft — the engine is unaffected by our outages).
type Subscriber struct {
	endpoint string
	topic    string
	dial     dialFunc
	backoff  time.Duration
	logger   *slog.Logger
}

// SubscriberOption configures a Subscriber.
type SubscriberOption func(*Subscriber)

// WithSubscriberBackoff sets the reconnect backoff (default 1s).
func WithSubscriberBackoff(d time.Duration) SubscriberOption {
	return func(s *Subscriber) { s.backoff = d }
}

// WithSubscriberLogger sets the logger (default slog.Default()).
func WithSubscriberLogger(l *slog.Logger) SubscriberOption {
	return func(s *Subscriber) { s.logger = l }
}

// NewSubscriber builds a Subscriber for one engine's ZMQ event endpoint
// (e.g. "tcp://127.0.0.1:5557") and topic (e.g. "kv-events"; "" = all topics).
func NewSubscriber(endpoint, topic string, opts ...SubscriberOption) *Subscriber {
	s := &Subscriber{
		endpoint: endpoint,
		topic:    topic,
		backoff:  time.Second,
		logger:   slog.Default(),
	}
	s.dial = func(ctx context.Context) (frameSource, error) { return dialZMQ(ctx, s.endpoint, s.topic) }
	for _, o := range opts {
		o(s)
	}
	if s.backoff <= 0 {
		s.backoff = time.Second // avoid a tight reconnect loop
	}
	return s
}

// Run connects, decodes batches, and sends them on out until ctx is cancelled.
// out is not closed (the caller owns it).
func (s *Subscriber) Run(ctx context.Context, out chan<- *EventBatch) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		src, err := s.dial(ctx)
		if err != nil {
			s.logger.Warn("zmq dial failed; retrying", "endpoint", s.endpoint, "err", err)
			if !sleepCtx(ctx, s.backoff) {
				return ctx.Err()
			}
			continue
		}
		s.logger.Info("subscribed to engine KV events", "endpoint", s.endpoint, "topic", s.topic)
		s.consume(ctx, src, out)
		_ = src.Close()
		if !sleepCtx(ctx, s.backoff) {
			return ctx.Err()
		}
	}
}

// consume reads frames until an error (then returns so Run reconnects) or ctx is
// cancelled. A malformed batch is logged and skipped, not fatal.
func (s *Subscriber) consume(ctx context.Context, src frameSource, out chan<- *EventBatch) {
	for {
		frames, err := src.Recv()
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("zmq recv failed; reconnecting", "err", err)
			}
			return
		}
		if len(frames) == 0 {
			continue
		}
		batch, err := DecodeEventBatch(frames[len(frames)-1])
		if err != nil {
			s.logger.Warn("dropping undecodable event batch", "err", err)
			continue
		}
		select {
		case out <- batch:
		case <-ctx.Done():
			return
		}
	}
}

// sleepCtx waits d or until ctx is cancelled; returns false if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// zmqSource adapts a zmq4 SUB socket to frameSource.
type zmqSource struct{ sock zmq4.Socket }

func (z *zmqSource) Recv() ([][]byte, error) {
	m, err := z.sock.Recv()
	if err != nil {
		return nil, err
	}
	return m.Frames, nil
}

func (z *zmqSource) Close() error { return z.sock.Close() }

// dialZMQ opens a SUB socket subscribed to topic and connected to endpoint.
func dialZMQ(ctx context.Context, endpoint, topic string) (frameSource, error) {
	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(endpoint); err != nil {
		_ = sub.Close() // Run retries forever; don't leak a socket per backoff cycle
		return nil, fmt.Errorf("zmq dial %s: %w", endpoint, err)
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, topic); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("zmq subscribe %q: %w", topic, err)
	}
	return &zmqSource{sock: sub}, nil
}
