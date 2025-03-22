package wmsqlitemodernc

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func TestSubscriber(t *testing.T) {
	topic := "test_topic"
	// connector := NewEphemeralConnector()
	connector := NewConnector("file:../mydata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite&&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared")
	pub, err := NewPublisher(PublisherConfiguration{
		Connector: connector,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("publish some messages", func(t *testing.T) {
		t.Parallel()
		for range 10 {
			msg := message.NewMessage(uuid.New().String(), []byte("test"))
			msg2 := message.NewMessage(uuid.New().String(), []byte("test"))
			if err := pub.Publish(topic, msg, msg2); err != nil {
				t.Fatal(err)
			}
		}
	})

	sub, err := NewSubscriber(SubscriberConfiguration{
		Connector: connector,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sub.(interface {
			Unsubscribe(string) error
		}).Unsubscribe(topic); err != nil {
			t.Fatal(err)
		}
	})

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

	end := time.After(time.Second * 2)
	for {
		select {
		case <-end:
			return
		case msg := <-msgs:
			if msg == nil {
				t.Fatal("message is nil")
			}
			msg.Ack()
			if string(msg.Payload) != "test" {
				t.Fatalf("unexpected message payload: %s", string(msg.Payload))
			}
		}
	}

	// for msg := range msgs {
	// 	if msg == nil {
	// 		t.Fatal("message is nil")
	// 	}
	// 	msg.Ack()
	// 	if string(msg.Payload) != "test" {
	// 		t.Fatalf("unexpected message payload: %s", string(msg.Payload))
	// 	}
	// }
}
