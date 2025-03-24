package tests

import (
	"bytes"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

func NewHung(setup PubSubFixture) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		pub, sub := setup(t, "hungTests")

		t.Run("make sure nack does not cause an inifite loop", hungNackTest(pub, sub))
	}
}

func hungNackTest(pub message.Publisher, sub message.Subscriber) func(t *testing.T) {
	topic := "hung-nack-test"
	return func(t *testing.T) {
		msgs, err := sub.Subscribe(t.Context(), topic)
		if err != nil {
			t.Fatal(err)
		}

		original := message.NewMessage("hungMessage", []byte("payload"))
		err = pub.Publish(
			topic,
			original,
			message.NewMessage("tailMessage", []byte("payload2")),
		)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case msg := <-msgs:
			if !bytes.Equal(msg.Payload, original.Payload) {
				t.Errorf("expected payload %q, got %q", original.Payload, msg.Payload)
			}
			// <-time.After(time.Second)
			msg.Nack()
		case <-time.After(time.Second):
			t.Fatal("first message was not received before nack")
		}

		select {
		case msg := <-msgs:
			if msg.UUID != original.UUID {
				t.Errorf("expected payload %q, got %q", original.UUID, msg.UUID)
			}
			// <-time.After(time.Second)
			msg.Ack()
		case <-time.After(time.Second):
			t.Fatal("second message was not received after nack")
		}

		select {
		case tail := <-msgs:
			tail.Ack()
			if tail.UUID != "tailMessage" {
				t.Errorf("expected payload %q, got %q", "tailMessage", tail.UUID)
			}
		case <-time.After(time.Second):
			t.Fatal("tail message was not never received")
		}
	}
}
