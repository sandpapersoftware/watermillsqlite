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
	ErrSubscriberClosed         = errors.New("subscriber is closed")
	ErrConsumerGroupIsLocked    = errors.New("consumer group is locked")
	ErrDestinationChannelIsBusy = errors.New("destination channel is busy")
)

type SubscriberOptions struct {
	ConsumerGroup string
	// InitializeSchema bool
	BatchSize           int
	Connector           Connector
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

	Logger watermill.LoggerAdapter
}

type subscriber struct {
	UUID                      string
	consumerGroup             string
	batchSize                 int
	connector                 Connector
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

func NewSubscriber(config SubscriberOptions) (message.Subscriber, error) {
	// TODO: validate config
	// TODO: validate consumer group - INJECTION
	// TODO: validate batch size
	// TODO: validate poll interval, and it must be less than lock timeout

	ackChannel := defaultAckChannel
	if config.AckDeadline != nil {
		deadline := *config.AckDeadline
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
	tng := config.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	return &subscriber{
		UUID:                      ID,
		consumerGroup:             config.ConsumerGroup,
		batchSize:                 cmp.Or(config.BatchSize, 10),
		connector:                 config.Connector,
		ackChannel:                ackChannel,
		closed:                    make(chan struct{}),
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		logger: cmp.Or[watermill.LoggerAdapter](
			config.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"subscriber_id":  ID,
			"consumer_group": config.ConsumerGroup,
		}),
		subscribeWg: &sync.WaitGroup{},
	}, nil
}

func (s *subscriber) Subscribe(ctx context.Context, topic string) (c <-chan *message.Message, err error) {
	select {
	case <-s.closed:
		return nil, ErrSubscriberClosed
	default:
	}

	db, err := s.connector.Connect()
	if err != nil {
		return nil, err
	}
	messagesTableName := s.TopicTableNameGenerator(topic)
	offsetsTableName := s.OffsetsTableNameGenerator(topic)
	if err = createTopicAndOffsetsTablesIfAbsent(
		ctx,
		db,
		messagesTableName,
		offsetsTableName,
	); err != nil {
		return nil, errors.Join(err, db.Close())
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO '%s' (consumer_group, offset_acked, locked_until)
		VALUES ("%s", 0, 0)
		ON CONFLICT(consumer_group) DO NOTHING;
	`, offsetsTableName, s.consumerGroup))
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}

	// TODO: customize batch size
	graceSeconds := 5 // TODO: customize grace period
	sub := &subscription{
		DB:           db,
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

func (s *subscriber) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
		s.subscribeWg.Wait()
	}
	return nil
}

func (s *subscriber) String() string {
	return "sqlite3-modernc-subscriber-" + s.UUID
}
