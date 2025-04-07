package wmsqlitemodernc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// SubscriberOptions defines options for creating a subscriber. Every selection has a reasonable default value.
type SubscriberOptions struct {
	// ConsumerGroup designates similar subscriptions to process messages from the same topic.
	// An empty consumer group is the default value. Messages are processed in batches.
	// Therefore, another subscriber with the same consumer group name may only obtain
	// messages whenever it is able to acquire the row lock.
	ConsumerGroup string

	// BatchSize is the number of messages to read in a single batch.
	// Default value is 100.
	BatchSize int

	// TableNameGenerators is a set of functions that generate table names for topics and offsets.
	// Defaults to [TableNameGenerators.WithDefaultGeneratorsInsteadOfNils].
	TableNameGenerators TableNameGenerators

	// PollInterval is the interval to wait between subsequent SELECT queries, if no more messages were found in the database (Prefer using the BackoffManager instead).
	// Must be non-negative. Defaults to one second.
	PollInterval time.Duration

	// AckDeadline is the time to wait for acking a message.
	// If message is not acked within this time, it will be nacked and re-delivered.
	//
	// When messages are read in bulk, this time is calculated for each message separately.
	//
	// If you want to disable ack deadline, set it to 0.
	// Warning: when ack deadline is disabled, messages may block the subscriber from reading new messages.
	//
	// Must be non-negative. Default value is 30 seconds.
	AckDeadline *time.Duration

	// InitializeSchema option enables initializing schema on making a subscription.
	InitializeSchema bool

	// Logger reports message consumption errors and traces. Defaults to [watermill.NewSlogLogger].
	Logger watermill.LoggerAdapter
}

type subscriber struct {
	DB                        SQLiteDatabase
	UUID                      string
	PollInterval              time.Duration
	InitializeSchema          bool
	ConsumerGroup             string
	BatchSize                 int
	NackChannel               func() <-chan time.Time
	Closed                    chan struct{}
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	Logger                    watermill.LoggerAdapter
	Subscriptions             *sync.WaitGroup
}

// NewSubscriber creates a new subscriber with the given options.
func NewSubscriber(db SQLiteDatabase, options SubscriberOptions) (message.Subscriber, error) {
	if db == nil {
		return nil, errors.New("database connection is nil")
	}
	if options.ConsumerGroup != "" {
		if err := validateTopicName(options.ConsumerGroup); err != nil {
			return nil, fmt.Errorf("consumer group name must follow the same validation rules are topic names: %w", err)
		}
	}
	if options.BatchSize < 0 {
		return nil, errors.New("BatchSize must be greater than 0")
	}
	if options.BatchSize > 1_000_000 {
		return nil, errors.New("BatchSize must be less than a million")
	}
	if options.PollInterval != 0 && options.PollInterval < time.Millisecond {
		return nil, errors.New("PollInterval must be greater than one millisecond")
	}
	if options.PollInterval > time.Hour*24*7 {
		return nil, errors.New("PollInterval must be less than a week")
	}

	nackChannel := func() <-chan time.Time {
		// by default, Nack messages if they take longer than 30 seconds to process
		return time.After(time.Second * 30)
	}
	if options.AckDeadline != nil {
		deadline := *options.AckDeadline
		if deadline < 0 {
			return nil, errors.New("AckDeadline must be above 0")
		}
		if deadline == 0 {
			nackChannel = func() <-chan time.Time {
				// infinite: always blocked
				return nil
			}
		} else {
			nackChannel = func() <-chan time.Time {
				return time.After(deadline)
			}
		}
	}

	ID := uuid.New().String()
	tng := options.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	return &subscriber{
		DB:                        db,
		UUID:                      ID,
		PollInterval:              cmpOrTODO(options.PollInterval, time.Second),
		InitializeSchema:          options.InitializeSchema,
		ConsumerGroup:             options.ConsumerGroup,
		BatchSize:                 cmpOrTODO(options.BatchSize, 100),
		NackChannel:               nackChannel,
		Closed:                    make(chan struct{}),
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		Logger: cmpOrTODO[watermill.LoggerAdapter](
			options.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"subscriber_id":  ID,
			"consumer_group": options.ConsumerGroup,
		}),
		Subscriptions: &sync.WaitGroup{},
	}, nil
}

// Subscribe streams messages from the topic. Satisfies [watermill.Subscriber] interface.
// Returns [io.ErrPipeClosed] if the subscriber is closed.
func (s *subscriber) Subscribe(ctx context.Context, topic string) (c <-chan *message.Message, err error) {
	if s.IsClosed() {
		return nil, io.ErrClosedPipe
	}

	messagesTableName := s.TopicTableNameGenerator(topic)
	offsetsTableName := s.OffsetsTableNameGenerator(topic)
	if s.InitializeSchema {
		if err = createTopicAndOffsetsTablesIfAbsent(
			ctx,
			s.DB,
			messagesTableName,
			offsetsTableName,
		); err != nil {
			return nil, err
		}
	}

	_, err = s.DB.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO '%s' (consumer_group, offset_acked, locked_until)
		VALUES ("%s", 0, 0)
		ON CONFLICT(consumer_group) DO NOTHING;
	`, offsetsTableName, s.ConsumerGroup))
	if err != nil {
		return nil, err
	}

	graceSeconds := 5 // TODO: customize grace period
	sub := &subscription{
		DB:           s.DB,
		pollTicker:   time.NewTicker(s.PollInterval),
		lockTicker:   time.NewTicker(time.Second * time.Duration(graceSeconds-1)),
		lockDuration: time.Second * time.Duration(graceSeconds-1),
		ackChannel:   s.NackChannel,

		sqlLockConsumerGroup: fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d) WHERE consumer_group="%s" AND locked_until < unixepoch() RETURNING offset_acked`, offsetsTableName, graceSeconds, s.ConsumerGroup),
		sqlExtendLock:        fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.ConsumerGroup),
		sqlNextMessageBatch: fmt.Sprintf(`
			SELECT "offset", uuid, payload, metadata
			FROM '%s'
			WHERE "offset">? ORDER BY offset LIMIT %d;
		`, messagesTableName, s.BatchSize),
		sqlAcknowledgeMessages: fmt.Sprintf(`
			UPDATE '%s' SET offset_acked=?, locked_until=0 WHERE consumer_group="%s" AND offset_acked = ?;
		`, offsetsTableName, s.ConsumerGroup),
		destination: make(chan *message.Message),
		logger: s.Logger.With(
			watermill.LogFields{
				"topic": topic,
			},
		),
	}

	ctx, cancel := context.WithCancel(ctx)
	go func(done <-chan struct{}) {
		<-done
		cancel()
	}(s.Closed)

	s.Subscriptions.Add(1)
	go func(ctx context.Context) {
		defer s.Subscriptions.Done()
		sub.Run(ctx)
		// <-time.After(time.Second) // give a chance for mid-air transaction to commit
		close(sub.destination)
		cancel()
	}(ctx)

	return sub.destination, nil
}

// IsClosed returns true if the subscriber is closed.
func (s *subscriber) IsClosed() bool {
	select {
	case <-s.Closed:
		return true
	default:
		return false
	}
}

// Close terminates the subscriber and all its associated resources. Returns when everything is closed.
func (s *subscriber) Close() error {
	if !s.IsClosed() {
		close(s.Closed)
		s.Subscriptions.Wait()
	}
	return nil
}

func (s *subscriber) String() string {
	return "sqlite3-modernc-subscriber-" + s.UUID
}
