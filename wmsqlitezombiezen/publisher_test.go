package wmsqlitezombiezen

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func TestPublisher(t *testing.T) {
	conn := newTestConnection(t)
	pub, err := NewPublisher(conn, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if err = pub.Publish(
		"test-topic",
		message.NewMessage(uuid.New().String(), []byte("test")),
		message.NewMessage(uuid.New().String(), []byte("test")),
	); err != nil {
		t.Fatal(err)
	}
}
