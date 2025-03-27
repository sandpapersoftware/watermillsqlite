# Watermill SQLite3 Driver Pack

Golang SQLite3 driver pack for Watermill event dispatcher. Drivers satisfy the following interfaces:

- message.Publisher
- message.Subscriber

## ModernC

```sh
go get -u github.com/dkotik/watermillsqlite/wmsqlitemodernc
```

ModernC driver is compatible with the Golang standard library SQL package. It works without CGO.

```go
import (
	"database/sql"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	_ "modernc.org/sqlite"
)

db, err := sql.Open("sqlite", ":memory:?journal_mode=WAL&busy_timeout=1000&cache=shared")
if err != nil {
	panic(err)
}
// limit the number of concurrent connections to one
// this is a limitation of `modernc.org/sqlite` driver
db.SetMaxOpenConns(1)
defer db.Close()

pub, err := wmsqlitemodernc.NewPublisher(db, wmsqlitemodernc.PublisherOptions{})
if err != nil {
	panic(err)
}
sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

## ZombieZen

```sh
go get -u github.com/dkotik/watermillsqlite/wmsqlitezombiezen
```

ZombieZen driver abandons the standard Golang library SQL conventions in favor of more orthogonal API and performance. Under the hood, it uses ModernC SQLite3 implementation and does not need CGO. It is faster than even the CGO variants.

```go
import (
	"database/sql"
	"github.com/dkotik/watermillsqlite/wmsqlitezombiezen"
	_ "modernc.org/sqlite"
)

connectionDSN := ":memory:?journal_mode=WAL&busy_timeout=1000&cache=shared")
conn, err := sqlite.OpenConn(connectionDSN)
if err != nil {
	panic(err)
}
defer conn.Close()

pub, err := wmsqlitezombiezen.NewPublisher(conn, wmsqlitezombiezen.PublisherOptions{})
if err != nil {
	panic(err)
}
sub, err := wmsqlitezombiezen.NewSubscriber(connectionDSN, wmsqlitezombiezen.SubscriberOptions{})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

## Development: Alpha Version

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations.

Current architectural decision is to lock a consumer group offset using `unixepoch()+graceTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `graceTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out.

- [ ] ModernC version repeated tests flake out usually when running with -count=5, but for a single run they pass.
- [x] Finish time-based lock extension when:
    - [x] sending a message to output channel
    - [x] waiting for message acknowledgement
- [ ] Add `NewDeduplicator` constructor for deduplication middleware.
- [x] Pass official implementation acceptance tests:
    - [x] ModernC
        - [x] tests.TestPublishSubscribe
        - [x] tests.TestConcurrentSubscribe
        - [x] tests.TestConcurrentSubscribeMultipleTopics
        - [x] tests.TestResendOnError
        - [x] tests.TestNoAck
        - [x] tests.TestContinueAfterSubscribeClose
        - [x] tests.TestConcurrentClose
        - [x] tests.TestContinueAfterErrors
        - [x] tests.TestPublishSubscribeInOrder
        - [x] tests.TestPublisherClose
        - [x] tests.TestTopic
        - [x] tests.TestMessageCtx
        - [x] tests.TestSubscribeCtx
        - [x] tests.TestConsumerGroups
    - [ ] ZombieZen (passes simple tests)
        - [ ] tests.TestPublishSubscribe
        - [ ] tests.TestConcurrentSubscribe
        - [ ] tests.TestConcurrentSubscribeMultipleTopics
        - [ ] tests.TestResendOnError
        - [ ] tests.TestNoAck
        - [ ] tests.TestContinueAfterSubscribeClose
        - [ ] tests.TestConcurrentClose
        - [ ] tests.TestContinueAfterErrors
        - [ ] tests.TestPublishSubscribeInOrder
        - [ ] tests.TestPublisherClose
        - [ ] tests.TestTopic
        - [ ] tests.TestMessageCtx
        - [ ] tests.TestSubscribeCtx
        - [ ] tests.TestConsumerGroups

## Similar Projects

- <https://github.com/davidroman0O/watermill-comfymill>
- <https://github.com/walterwanderley/watermill-sqlite>
<!-- - <https://github.com/ov2b/watermill-sqlite3> - author requested removal of the mention, because it is a very rough draft - requires CGO for `mattn/go-sqlite3` dependency -->
