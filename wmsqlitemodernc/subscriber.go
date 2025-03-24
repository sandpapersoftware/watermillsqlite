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
)

var (
	ErrClosed = errors.New("subscriber is closed")
)

type Subscriber interface {
	message.Subscriber
	Unsubscribe(topic string) error
}

type SubscriberConfiguration struct {
	ConsumerGroup             string
	BatchSize                 int
	GenerateMessagesTableName TableNameGenerator
	GenerateOffsetsTableName  TableNameGenerator
	Connector                 Connector
	PollInterval              time.Duration

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
	consumerGroup             string
	batchSize                 int
	connector                 Connector
	ackChannel                func() <-chan time.Time
	closed                    chan struct{}
	generateMessagesTableName TableNameGenerator
	generateOffsetsTableName  TableNameGenerator
	logger                    watermill.LoggerAdapter

	mu                   sync.Mutex
	subscriptionsByTopic map[string]*subscription
}

func infiniteAckChannel() <-chan time.Time {
	return nil
}

func defaultAckChannel() <-chan time.Time {
	return time.After(time.Second * 30)
}

func NewSubscriber(cfg SubscriberConfiguration) (Subscriber, error) {
	// TODO: validate config
	// TODO: validate consumer group - INJECTION
	// TODO: validate batch size
	// TODO: validate poll interval, and it must be less than lock timeout

	ackChannel := defaultAckChannel
	if cfg.AckDeadline != nil {
		deadline := *cfg.AckDeadline
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
	return &subscriber{
		consumerGroup: cfg.ConsumerGroup,
		batchSize:     cmp.Or(cfg.BatchSize, 10),
		connector:     cfg.Connector,
		ackChannel:    ackChannel,
		closed:        make(chan struct{}),
		generateMessagesTableName: cmp.Or(
			cfg.GenerateMessagesTableName,
			DefaultMessagesTableNameGenerator,
		),
		generateOffsetsTableName: cmp.Or(
			cfg.GenerateOffsetsTableName,
			DefaultOffsetsTableNameGenerator,
		),
		logger: cmp.Or[watermill.LoggerAdapter](
			cfg.Logger,
			watermill.NewSlogLogger(nil),
		),
		mu:                   sync.Mutex{},
		subscriptionsByTopic: make(map[string]*subscription),
	}, nil
}

func (s *subscriber) Subscribe(ctx context.Context, topic string) (c <-chan *message.Message, err error) {
	select {
	case <-s.closed:
		return nil, ErrClosed
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	matched, ok := s.subscriptionsByTopic[topic]
	if ok {
		return matched.destination, nil
	}

	db, err := s.connector.Connect()
	if err != nil {
		return nil, err
	}
	messagesTableName := s.generateMessagesTableName.GenerateTableName(topic)
	offsetsTableName := s.generateOffsetsTableName.GenerateTableName(topic)
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
	matched = &subscription{
		db:                   db,
		pollTicker:           time.NewTicker(time.Millisecond * 120),
		ackChannel:           s.ackChannel,
		sqlLockConsumerGroup: fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d) WHERE consumer_group="%s" AND locked_until < unixepoch() RETURNING COALESCE(offset_acked, 0)`, offsetsTableName, graceSeconds, s.consumerGroup),
		sqlExtendLock:        fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.consumerGroup),
		// TODO: remove created_at ?
		sqlNextMessageBatch: fmt.Sprintf(`
			SELECT
				"offset", uuid, payload, metadata
			FROM '%s'
			WHERE "offset" > (
				SELECT offset_acked FROM '%s' WHERE consumer_group = "%s"
			)
			ORDER BY offset LIMIT %d;
		`, messagesTableName, offsetsTableName, s.consumerGroup, s.batchSize),
		sqlAcknowledgeMessages: fmt.Sprintf(`
			UPDATE '%s' SET offset_acked=?, locked_until=0 WHERE consumer_group = "%s" AND offset_acked = ?;
		`, offsetsTableName, s.consumerGroup),
		destination: make(chan *message.Message),
		logger:      s.logger, // TODO: logger.With
	}
	s.subscriptionsByTopic[topic] = matched
	go matched.Loop(s.closed)
	return matched.destination, nil
}

func (s *subscriber) Unsubscribe(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, ok := s.subscriptionsByTopic[topic]
	if !ok {
		return nil
	}
	if err := subscription.Close(); err != nil {
		return err
	}
	delete(s.subscriptionsByTopic, topic)
	return nil
}

func (s *subscriber) Close() (err error) {
	if s.closed == nil {
		return nil
	}
	close(s.closed)
	s.closed = nil
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subscriptionsByTopic {
		err = errors.Join(err, sub.Close())
	}
	return err
}

func (s *subscriber) String() string {
	return "sqlite3-modernc-subscriber"
}
