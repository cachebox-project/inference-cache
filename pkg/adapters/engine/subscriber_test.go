package engine

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// fakeSource replays canned multipart frame sets, then returns io.EOF.
type fakeSource struct {
	sets [][][]byte
	i    int
}

func (f *fakeSource) Recv() ([][]byte, error) {
	if f.i >= len(f.sets) {
		return nil, io.EOF
	}
	s := f.sets[f.i]
	f.i++
	return s, nil
}

func (f *fakeSource) Close() error { return nil }

// frameSet wraps a payload as a vLLM-style multipart message [topic, seq, payload].
func frameSet(topic string, payload []byte) [][]byte {
	return [][]byte{[]byte(topic), {0, 0, 0, 0, 0, 0, 0, 1}, payload}
}

func TestSubscriberDecodesAndForwards(t *testing.T) {
	valid := encodeVLLMBatch(t, 5.0,
		[]interface{}{"BlockStored", []uint64{99}, nil, []int64{}, int32(16), nil})

	out := make(chan *EventBatch, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := NewSubscriber("tcp://unused", "kv-events", WithSubscriberBackoff(time.Millisecond))
	calls := 0
	sub.dial = func(context.Context) (frameSource, error) {
		calls++
		if calls == 1 {
			// A garbage batch (must be skipped) followed by a valid one.
			return &fakeSource{sets: [][][]byte{
				frameSet("kv-events", []byte{0xff, 0x00}),
				frameSet("kv-events", valid),
			}}, nil
		}
		cancel() // stop after the first reconnect
		return nil, errors.New("stop")
	}

	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx, out) }()

	select {
	case b := <-out:
		if len(b.Events) != 1 {
			t.Fatalf("got %d events, want 1", len(b.Events))
		}
		stored, ok := b.Events[0].(BlockStored)
		if !ok || stored.BlockHashes[0] != 99 {
			t.Fatalf("event = %#v, want BlockStored{99}", b.Events[0])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no batch forwarded")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after ctx cancel")
	}
	if calls < 2 {
		t.Errorf("expected a reconnect attempt (dial calls=%d)", calls)
	}
}
