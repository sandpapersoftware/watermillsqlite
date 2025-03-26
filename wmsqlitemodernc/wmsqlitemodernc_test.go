package wmsqlitemodernc_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc/tests"
	_ "modernc.org/sqlite"
)

var ephemeralDB = NewPubSubFixture(wmsqlitemodernc.NewGlobalInMemoryEphemeralConnector(context.Background()))

func NewPubSubFixture(db wmsqlitemodernc.SQLiteDatabase) tests.PubSubFixture {
	// &_txlock=exclusive
	// connector := wmsqlitemodernc.NewConnector(fmt.Sprintf(
	// 	"file://%s/%s?mode=memory&journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared",
	// 	t.TempDir(),
	// 	"db.sqlite3",
	// ))
	// connector := wmsqlitemodernc.NewGlobalInMemoryEphemeralConnector(t.Context())

	return func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		pub, err := wmsqlitemodernc.NewPublisher(
			db,
			wmsqlitemodernc.PublisherOptions{
				ParentContext: t.Context(),
			})
		if err != nil {
			t.Fatal("unable to initialize publisher:", err)
		}
		t.Cleanup(func() {
			if err := pub.Close(); err != nil {
				t.Fatal(err)
			}
		})

		sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{
			ConsumerGroup: consumerGroup,
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
	t.Run("basic functionality", tests.TestBasicSendRecieve(ephemeralDB))
	t.Run("one publisher three subscribers", tests.TestOnePublisherThreeSubscribers(ephemeralDB, 1000))
	t.Run("perpetual locks", tests.TestHungOperations(ephemeralDB))
}

func TestOfficialImplementationAcceptance(t *testing.T) {
	file, err := sql.Open("sqlite", fmt.Sprintf(
		"file://%s/%s?mode=memory&journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared",
		t.TempDir(),
		"db.sqlite3",
	))
	if err != nil {
		t.Fatal("unable to open database:", err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Fatal("unable to close test SQLite database", err)
		}
	})

	if testing.Short() {
		t.Skip("acceptance tests take several minutes to complete for all file and memory bound transactions")
	}
	t.Run("file bound transactions", tests.OfficialImplementationAcceptance(NewPubSubFixture(
		file,
	)))
	t.Run("memory bound transactions", tests.OfficialImplementationAcceptance(ephemeralDB))
}
