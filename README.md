# Watermill SQLite3 Driver Pack

Golang SQLite3 driver pack for <watermill.io> event bus. Drivers satisfy the following interfaces:

- [message.Publisher](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill@v1.4.6/message#Publisher)
- [message.Subscriber](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill@v1.4.6/message#Subscriber)

## ModernC
[![Go Reference](https://pkg.go.dev/badge/github.com/ThreeDotsLabs/watermill.svg)](https://pkg.go.dev/github.com/dkotik/watermillsqlite/wmsqlitemodernc)
[![Go Report Card](https://goreportcard.com/badge/github.com/dkotik/watermillsqlite/wmsqlitemodernc)](https://goreportcard.com/report/github.com/dkotik/watermillsqlite/wmsqlitemodernc)
[![codecov](https://codecov.io/gh/dkotik/watermillsqlite/wmsqlitemodernc/branch/master/graph/badge.svg)](https://codecov.io/gh/dkotik/watermillsqlite/wmsqlitemodernc)

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

pub, err := wmsqlitemodernc.NewPublisher(db, wmsqlitemodernc.PublisherOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

## ZombieZen
[![Go Reference](https://pkg.go.dev/badge/github.com/ThreeDotsLabs/watermill.svg)](https://pkg.go.dev/github.com/dkotik/watermillsqlite/wmsqlitezombiezen)
[![Go Report Card](https://goreportcard.com/badge/github.com/dkotik/watermillsqlite/wmsqlitezombiezen)](https://goreportcard.com/report/github.com/dkotik/watermillsqlite/wmsqlitezombiezen)
[![codecov](https://codecov.io/gh/dkotik/watermillsqlite/wmsqlitezombiezen/branch/master/graph/badge.svg)](https://codecov.io/gh/dkotik/watermillsqlite/wmsqlitezombiezen)

```sh
go get -u github.com/dkotik/watermillsqlite/wmsqlitezombiezen
```

ZombieZen driver abandons the standard Golang library SQL conventions in favor of more orthogonal API and performance. Under the hood, it uses ModernC SQLite3 implementation and does not need CGO. It is about **6 times faster** than the ModernC driver. And, it is currently more stable due to lower level control. It is faster than even the CGO SQLite variants.

```go
import "github.com/dkotik/watermillsqlite/wmsqlitezombiezen"

// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643
connectionDSN := ":memory:?journal_mode=WAL&busy_timeout=1000&cache=shared")
conn, err := sqlite.OpenConn(connectionDSN)
if err != nil {
	panic(err)
}
defer conn.Close()

pub, err := wmsqlitezombiezen.NewPublisher(conn, wmsqlitezombiezen.PublisherOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
sub, err := wmsqlitezombiezen.NewSubscriber(connectionDSN, wmsqlitezombiezen.SubscriberOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

## Development Roadmap

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations.

Current architectural decision is to lock a consumer group offset using `unixepoch()+lockTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `lockTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out. A time field solution may appear primitive, but databases use a network timeout to terminate transactions when disconnecting. Its presence is concealed because the logic resides in a lower stack level.

- [ ] ModernC version repeated tests flake out usually when running with -count=5, but for a single run they pass.
- [ ] Add lock timeout option.
- [ ] Add clean up routines for removing old messages from topics.
    - [ ] wmsqlitemodernc.CleanUpTopics
    - [ ] wmsqlitezombiezen.CleanUpTopics
- [ ] Add `NewDeduplicator` constructor for deduplication middleware.
- [x] Finish time-based lock extension when:
    - [x] sending a message to output channel
    - [x] waiting for message acknowledgement
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
    - [x] ZombieZen (passes simple tests)
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

## Similar Projects

- <https://github.com/davidroman0O/watermill-comfymill>
- <https://github.com/walterwanderley/watermill-sqlite>
<!-- - <https://github.com/ov2b/watermill-sqlite3> - author requested removal of the mention, because it is a very rough draft - requires CGO for `mattn/go-sqlite3` dependency -->
