# Watermill SQLite3 Driver Pack

Golang SQLite3 driver pack for Watermill event dispatcher. Drivers satisfy message Publisher and Subscriber interfaces.

## Development

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations.

Current architectural decision is to lock a consumer group offset using `unixepoch()+graceTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `graceTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over lock as soon as the grace period runs out.

- Package is early alpha.
- [ ] Ephemeral in-memory `Connector` must also satisfy tests.
- [ ] add `NewDeduplicator` constructor for deduplication middleware.
