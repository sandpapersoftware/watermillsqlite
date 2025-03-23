package wmsqlitemodernc_test

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	_ "modernc.org/sqlite"
)

var features = tests.Features{
	// ConsumerGroups should be true, if consumer groups are supported.
	ConsumerGroups: false,

	// ExactlyOnceDelivery should be true, if exactly-once delivery is supported.
	ExactlyOnceDelivery: false,

	// GuaranteedOrder should be true, if order of messages is guaranteed.
	GuaranteedOrder: true,

	// Some Pub/Subs guarantee the order only when one subscriber is subscribed at a time.
	GuaranteedOrderWithSingleSubscriber: false,

	// Persistent should be true, if messages are persistent between multiple instances of a Pub/Sub
	// (in practice, only GoChannel doesn't support that).
	Persistent: false,

	// RequireSingleInstance must be true,if a PubSub requires a single instance to work properly
	// (for example: GoChannel implementation).
	RequireSingleInstance: true,

	// NewSubscriberReceivesOldMessages should be set to true if messages are persisted even
	// if they are already consumed (for example, like in Kafka).
	NewSubscriberReceivesOldMessages: false,

	// GenerateTopicFunc overrides standard topic name generation.
	// GenerateTopicFunc func(tctx TestContext) string
}

/*
The name may be a filename, e.g., "/tmp/mydata.sqlite", or a URI, in which case it may include a '?' followed by one or more query parameters. For example, "file:///tmp/mydata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite". The supported query parameters are:

_pragma: Each value will be run as a "PRAGMA ..." statement (with the PRAGMA keyword added for you). May be specified more than once, '&'-separated. For more information on supported PRAGMAs see: https://www.sqlite.org/pragma.html

_time_format: The name of a format to use when writing time values to the database. Currently the only supported value is "sqlite", which corresponds to format 7 from https://www.sqlite.org/lang_datefunc.html#time_values, including the timezone specifier. If this parameter is not specified, then the default String() format will be used.

_txlock: The locking behavior to use when beginning a transaction. May be "deferred" (the default), "immediate", or "exclusive" (case insensitive). See: https://www.sqlite.org/lang_transaction.html#deferred_immediate_and_exclusive_transactions
*/

func TestOfficialImplementationAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance tests in short mode")
	}
	t.Run("ephemeral", func(t *testing.T) {
		t.Skip("ephemeral read is blocking")
		tests.TestPubSub(
			t,
			features,
			func(t *testing.T) (message.Publisher, message.Subscriber) {
				connector := wmsqlitemodernc.NewEphemeralConnector()
				pub, err := wmsqlitemodernc.NewPublisher(wmsqlitemodernc.PublisherConfiguration{
					Connector: connector,
				})
				if err != nil {
					t.Fatal("unable to initialize publisher:", err)
				}
				sub, err := wmsqlitemodernc.NewSubscriber(wmsqlitemodernc.SubscriberConfiguration{
					Connector: connector,
				})
				if err != nil {
					t.Fatal("unable to initialize publisher:", err)
				}
				return pub, sub
			},
			nil, // TODO: consumer group support
		)
	})

	t.Run("disk", func(t *testing.T) {
		tests.TestPubSub(
			t,
			features,
			func(t *testing.T) (message.Publisher, message.Subscriber) {
				connector := wmsqlitemodernc.NewConnector(fmt.Sprintf(
					"file://%s/%s?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared&_txlock=exclusive",
					t.TempDir(),
					"db.sqlite3",
				))

				pub, err := wmsqlitemodernc.NewPublisher(wmsqlitemodernc.PublisherConfiguration{
					Connector: connector,
				})
				if err != nil {
					t.Fatal("unable to initialize publisher:", err)
				}
				sub, err := wmsqlitemodernc.NewSubscriber(wmsqlitemodernc.SubscriberConfiguration{
					Connector: connector,
				})
				if err != nil {
					t.Fatal("unable to initialize publisher:", err)
				}
				return pub, sub
			},
			nil, // TODO: consumer group support
		)
	})
}
