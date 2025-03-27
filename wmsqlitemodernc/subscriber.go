package wmsqlitemodernc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

var (
	ErrSubscriberIsClosed = errors.New("subscriber is closed")
)

type SubscriberOptions struct {
	ConsumerGroup string
	// InitializeSchema option enables initializing schema on making subscription.
	InitializeSchema bool

	// BatchSize is the number of messages to read in a single batch.
	// Defaults to 10.
	// TODO: batch size of 100 fails to pass tests: investigate.
	BatchSize int

	// TableNameGenerators is a set of functions that generate table names for topics and offsets.
	// Defaults to [TableNameGenerators.WithDefaultGeneratorsInsteadOfNils].
	TableNameGenerators TableNameGenerators
	PollInterval        time.Duration

	// AckDeadline is the time to wait for acking a message.
	// If message is not acked within this time, it will be nacked and re-delivered.
	//
	// When messages are read in bulk, this time is calculated for each message separately.
	//
	// If you want to disable ack deadline, set it to 0.
	// Warning: when ack deadline is disabled, messages may block the subscriber from reading new messages.
	//
	// Must be non-negative. Nil value defaults to 30s.
	AckDeadline *time.Duration

	// Logger reports message consumption errors and traces. Defaults value is [watermill.NewSlogLogger].
	Logger watermill.LoggerAdapter
}

type subscriber struct {
	DB                        SQLiteDatabase
	UUID                      string
	consumerGroup             string
	batchSize                 int
	ackChannel                func() <-chan time.Time
	closed                    chan struct{}
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	logger                    watermill.LoggerAdapter

	subscribeWg *sync.WaitGroup
}

func infiniteAckChannel() <-chan time.Time {
	return nil
}

func defaultAckChannel() <-chan time.Time {
	return time.After(time.Second * 30)
}

func NewSubscriber(db SQLiteDatabase, options SubscriberOptions) (message.Subscriber, error) {
	if db == nil {
		return nil, errors.New("database connection is nil")
	}
	// TODO: validate config
	// TODO: validate consumer group - INJECTION
	// TODO: validate batch size
	// TODO: validate poll interval, and it must be less than lock timeout

	ackChannel := defaultAckChannel
	if options.AckDeadline != nil {
		deadline := *options.AckDeadline
		if deadline < 0 {
			return nil, errors.New("AckDeadline must be above 0")
		}
		if deadline == 0 {
			ackChannel = infiniteAckChannel
		} else {
			ackChannel = func() <-chan time.Time {
				return time.After(deadline)
			}
		}
	}

	ID := uuid.New().String()
	tng := options.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	return &subscriber{
		DB:                        db,
		UUID:                      ID,
		consumerGroup:             options.ConsumerGroup,
		batchSize:                 cmp.Or(options.BatchSize, 10),
		ackChannel:                ackChannel,
		closed:                    make(chan struct{}),
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		logger: cmp.Or[watermill.LoggerAdapter](
			options.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"subscriber_id":  ID,
			"consumer_group": options.ConsumerGroup,
		}),
		subscribeWg: &sync.WaitGroup{},
	}, nil
}

func (s *subscriber) Subscribe(ctx context.Context, topic string) (c <-chan *message.Message, err error) {
	if s.IsClosed() {
		return nil, ErrSubscriberIsClosed
	}

	messagesTableName := s.TopicTableNameGenerator(topic)
	offsetsTableName := s.OffsetsTableNameGenerator(topic)
	if err = createTopicAndOffsetsTablesIfAbsent(
		ctx,
		s.DB,
		messagesTableName,
		offsetsTableName,
	); err != nil {
		return nil, err
	}

	_, err = s.DB.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO '%s' (consumer_group, offset_acked, locked_until)
		VALUES ("%s", 0, 0)
		ON CONFLICT(consumer_group) DO NOTHING;
	`, offsetsTableName, s.consumerGroup))
	if err != nil {
		return nil, err
	}

	// TODO: customize batch size
	graceSeconds := 5 // TODO: customize grace period
	sub := &subscription{
		DB:           s.DB,
		pollTicker:   time.NewTicker(time.Millisecond * 20),
		lockTicker:   time.NewTicker(time.Second * time.Duration(graceSeconds-1)),
		lockDuration: time.Second * time.Duration(graceSeconds-1),
		ackChannel:   s.ackChannel,

		sqlLockConsumerGroup: fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d) WHERE consumer_group="%s" AND locked_until < unixepoch() RETURNING offset_acked`, offsetsTableName, graceSeconds, s.consumerGroup),
		sqlExtendLock:        fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.consumerGroup),
		sqlNextMessageBatch: fmt.Sprintf(`
			SELECT "offset", uuid, payload, metadata
			FROM '%s'
			WHERE "offset">? ORDER BY offset LIMIT %d;
		`, messagesTableName, s.batchSize),
		sqlAcknowledgeMessages: fmt.Sprintf(`
			UPDATE '%s' SET offset_acked=?, locked_until=0 WHERE consumer_group = "%s" AND offset_acked = ?;
		`, offsetsTableName, s.consumerGroup),
		destination: make(chan *message.Message),
		logger: s.logger.With(
			watermill.LogFields{
				"topic": topic,
			},
		),
	}

	ctx, cancel := context.WithCancel(ctx)
	go func(done <-chan struct{}) {
		<-done
		cancel()
	}(s.closed)

	s.subscribeWg.Add(1)
	go func(ctx context.Context) {
		defer s.subscribeWg.Done()
		sub.Run(ctx)
		close(sub.destination)
		cancel()
	}(ctx)

	return sub.destination, nil
}

func (s *subscriber) IsClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *subscriber) Close() error {
	if !s.IsClosed() {
		close(s.closed)
		s.subscribeWg.Wait()
	}
	return nil
}

func (s *subscriber) String() string {
	return "sqlite3-modernc-subscriber-" + s.UUID
}
