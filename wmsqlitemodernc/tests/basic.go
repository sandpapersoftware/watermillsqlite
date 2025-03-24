package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func NewBasic(setup PubSubFixture) func(t *testing.T) {
	topic := "test-basic-publish-subscribe"
	return func(t *testing.T) {
		pub, sub := setup(t, "")
		t.Cleanup(func() {
			if err := sub.(interface {
				Unsubscribe(string) error
			}).Unsubscribe(topic); err != nil {
				t.Fatal(err)
			}
			if err := sub.Close(); err != nil {
				t.Fatal(err)
			}
		})
		t.Cleanup(func() {
			if err := pub.Close(); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("publish 20 messages", func(t *testing.T) {
			t.Parallel()

			for range 10 {
				msg := message.NewMessage(uuid.New().String(), []byte("test"))
				msg2 := message.NewMessage(uuid.New().String(), []byte("test"))
				if err := pub.Publish(topic, msg, msg2); err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Run("collect 20 messages", func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			msgs, err := sub.Subscribe(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case <-time.After(time.Second):
				t.Fatal("first message took longer than a second")
			case msg := <-msgs:
				if msg == nil {
					t.Fatal("message is nil")
				}
				msg.Ack()
				if string(msg.Payload) != "test" {
					t.Fatalf("unexpected message payload: %s", string(msg.Payload))
				}
			}

			end := time.After(time.Second * 4)
			processed := 0

		loop:
			for {
				select {
				case <-end:
					break loop
				case msg := <-msgs:
					if msg == nil {
						t.Fatal("message is nil")
					}
					msg.Ack()
					if string(msg.Payload) != "test" {
						t.Fatalf("unexpected message payload: %s", string(msg.Payload))
					}
					processed++
				}
			}
			if processed != 19 {
				t.Fatalf("expected 19 messages, got %d", processed)
			}
		})
	}
}
