package wmsqlitemodernc_test

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc/tests"
	_ "modernc.org/sqlite"
)

func NewPubSubFixture(t *testing.T) tests.PubSubFixture {
	// &_txlock=exclusive
	connector := wmsqlitemodernc.NewConnector(fmt.Sprintf(
		"file://%s/%s?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared",
		t.TempDir(),
		"db.sqlite3",
	))

	return func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		pub, err := wmsqlitemodernc.NewPublisher(wmsqlitemodernc.PublisherConfiguration{
			Connector: connector,
		})
		if err != nil {
			t.Fatal("unable to initialize publisher:", err)
		}
		t.Cleanup(func() {
			if err := pub.Close(); err != nil {
				t.Fatal(err)
			}
		})

		sub, err := wmsqlitemodernc.NewSubscriber(wmsqlitemodernc.SubscriberConfiguration{
			Connector: connector,
		})
		if err != nil {
			t.Fatal("unable to initialize publisher:", err)
		}
		t.Cleanup(func() {
			if err := sub.Close(); err != nil {
				t.Fatal(err)
			}
		})

		return pub, sub
	}
}

func TestPubSub(t *testing.T) {
	setup := NewPubSubFixture(t)
	t.Run("basic functionality", tests.NewBasic(setup))
	t.Run("perpetual locks", tests.NewHung(setup))
}

func TestOfficialImplementationAcceptance(t *testing.T) {
	tests.OfficialImplementationAcceptance(NewPubSubFixture(t))(t)
}
