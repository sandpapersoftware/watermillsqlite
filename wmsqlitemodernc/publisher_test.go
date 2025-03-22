package wmsqlitemodernc

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func TestPublisher(t *testing.T) {
	pub, err := NewPublisher(PublisherConfiguration{
		Connector: NewEphemeralConnector(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Fatal(err)
		}
	})

	for range 10 {
		msg := message.NewMessage(uuid.New().String(), []byte("test"))
		msg2 := message.NewMessage(uuid.New().String(), []byte("test"))
		if err := pub.Publish("test_topic", msg, msg2); err != nil {
			t.Fatal(err)
		}
	}
}
