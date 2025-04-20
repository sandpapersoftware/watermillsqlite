package wmsqlitemodernc

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc/tests"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func newTestConnection(t *testing.T, connectionDSN string) *sql.DB {
	db, err := sql.Open("sqlite", connectionDSN)
	if err != nil {
		t.Fatal("unable to create test SQLite connetion", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal("unable to close test SQLite connetion", err)
		}
	})
	return db
}

func NewPubSubFixture(connectionDSN string) tests.PubSubFixture {
	return func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		publisherDB := newTestConnection(t, connectionDSN)

		pub, err := NewPublisher(
			publisherDB,
			PublisherOptions{
				InitializeSchema: true,
			})
		if err != nil {
			t.Fatal("unable to initialize publisher:", err)
		}
		t.Cleanup(func() {
			if err := pub.Close(); err != nil {
				t.Fatal(err)
			}
		})

		subscriberDB := newTestConnection(t, connectionDSN)
		sub, err := NewSubscriber(subscriberDB, SubscriberOptions{
			PollInterval:     time.Millisecond * 20,
			ConsumerGroup:    consumerGroup,
			InitializeSchema: true,
		})
		if err != nil {
			t.Fatal("unable to initialize subscriber:", err)
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
	return NewPubSubFixture("file:" + uuid.New().String() + "?mode=memory&journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
}

func NewFileDB(t *testing.T) tests.PubSubFixture {
	file := filepath.Join(t.TempDir(), uuid.New().String()+".sqlite3")
	t.Cleanup(func() {
		if err := os.Remove(file); err != nil {
			t.Fatal("unable to remove test SQLite database file", err)
		}
	})
	// &_txlock=exclusive
	return NewPubSubFixture("file:" + file + "?journal_mode=WAL&busy_timeout=5000&secure_delete=true&foreign_keys=true&cache=shared")
}

func TestPubSub(t *testing.T) {
	// if !testing.Short() {
	// 	t.Skip("working on acceptance tests")
	// }
	inMemory := NewEphemeralDB(t)
	t.Run("basic functionality", tests.TestBasicSendRecieve(inMemory))
	t.Run("one publisher three subscribers", tests.TestOnePublisherThreeSubscribers(inMemory, 1000))
	t.Run("perpetual locks", tests.TestHungOperations(inMemory))
}

func TestOfficialImplementationAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance tests take several minutes to complete for all file and memory bound transactions")
	}
	t.Run("file bound transactions", tests.OfficialImplementationAcceptance(NewFileDB(t)))
	t.Run("memory bound transactions", tests.OfficialImplementationAcceptance(NewEphemeralDB(t)))
}
