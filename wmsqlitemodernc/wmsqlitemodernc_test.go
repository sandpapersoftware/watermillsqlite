package wmsqlitemodernc_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc/tests"
	_ "modernc.org/sqlite"
)

func NewPubSubFixture(db wmsqlitemodernc.SQLiteDatabase) tests.PubSubFixture {
	return func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		pub, err := wmsqlitemodernc.NewPublisher(
			db,
			wmsqlitemodernc.PublisherOptions{
				ParentContext:        t.Context(),
				AutoInitializeSchema: true,
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

func NewEphemeralDB(t *testing.T) tests.PubSubFixture {
	// &_txlock=exclusive
	db, err := sql.Open("sqlite",
		":memory:?journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared",
	)
	db.SetMaxOpenConns(1)
	if err != nil {
		t.Fatal("unable to open database:", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal("unable to close test SQLite database", err)
		}
	})
	return NewPubSubFixture(db)
}

func NewFilelDB(t *testing.T) tests.PubSubFixture {
	// &_txlock=exclusive
	file := filepath.Join(t.TempDir(), "db.sqlite3")
	db, err := sql.Open("sqlite",
		"file:"+file+"?journal_mode=WAL&busy_timeout=5000&secure_delete=true&foreign_keys=true&cache=shared",
	)
	db.SetMaxOpenConns(1)
	if err != nil {
		t.Fatal("unable to open database:", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal("unable to close test SQLite database", err)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal("unable to remove test SQLite database file", err)
		}
	})
	return NewPubSubFixture(db)
}

func TestPubSub(t *testing.T) {
	inMemory := NewEphemeralDB(t)

	t.Run("basic functionality", tests.TestBasicSendRecieve(inMemory))
	t.Run("one publisher three subscribers", tests.TestOnePublisherThreeSubscribers(inMemory, 1000))
	t.Run("perpetual locks", tests.TestHungOperations(inMemory))
}

func TestOfficialImplementationAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance tests take several minutes to complete for all file and memory bound transactions")
	}
	t.Run("file bound transactions", tests.OfficialImplementationAcceptance(NewFilelDB(t)))
	t.Run("memory bound transactions", tests.OfficialImplementationAcceptance(NewEphemeralDB(t)))
}
