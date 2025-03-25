package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func TestBasicSendRecieve(setup PubSubFixture) func(t *testing.T) {
	topic := "test-basic-publish-subscribe"
	return func(t *testing.T) {
		pub, sub := setup(t, "")
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

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
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

func TestOnePublisherThreeSubscribers(setup PubSubFixture, messageCount int) func(t *testing.T) {
	topic := "one-publisher-three-subscribers"
	return func(t *testing.T) {
		t.Parallel()

		pub, sub := setup(t, "")
		t.Run("publish messages", func(t *testing.T) {
			t.Parallel()
			for range messageCount {
				msg := message.NewMessage(uuid.New().String(), []byte("test"))
				if err := pub.Publish(topic, msg); err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Run("collect messages", func(t *testing.T) {
			t.Parallel()
			first, err := sub.Subscribe(t.Context(), topic)
			if err != nil {
				t.Fatal(err)
			}
			second, err := sub.Subscribe(t.Context(), topic)
			if err != nil {
				t.Fatal(err)
			}
			third, err := sub.Subscribe(t.Context(), topic)
			if err != nil {
				t.Fatal(err)
			}

			collected := 0
			done := time.After(time.Second * 2)
			for {
				select {
				case <-done:
					if collected != messageCount {
						t.Fatalf("expected %d messages, got %d", messageCount, collected)
					}
					return
				case msg := <-first:
					msg.Ack()
					collected++
				case msg := <-second:
					msg.Ack()
					collected++
				case msg := <-third:
					msg.Ack()
					collected++
				}
			}
		})
	}
}
