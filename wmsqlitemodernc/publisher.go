package wmsqlitemodernc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// PublisherOptions customize message publishing behavior.
type PublisherOptions struct {
	// ParentContext is the context used for deriving all internal publishing operations.
	// Default value is [context.Background].
	ParentContext context.Context

	// TableNameGenerators is a set of functions that generate table names for topics and offsets.
	// Defaults to [TableNameGenerators.WithDefaultGeneratorsInsteadOfNils].
	TableNameGenerators TableNameGenerators

	// InitializeSchema enables initialization of schema database during publish.
	// Schema is initialized once per topic per publisher instance.
	// InitializeSchema is forbidden if using an ongoing transaction as database handle.
	// It could result in an implicit commit of the transaction by a CREATE TABLE statement.
	InitializeSchema bool

	// Logger reports message publishing errors and traces. Defaults value is [watermill.NewSlogLogger].
	Logger watermill.LoggerAdapter
}

type publisher struct {
	Context                   context.Context
	ContextCancel             context.CancelCauseFunc
	InitializeSchema          bool
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	UUID                      string
	DB                        SQLiteConnection
	Logger                    watermill.LoggerAdapter

	mu          sync.Mutex
	knownTopics map[string]struct{}
}

// NewPublisher creates a [message.Publisher] instance from a connection interface which could be
// a database handle or a transaction.
func NewPublisher(db SQLiteConnection, options PublisherOptions) (message.Publisher, error) {
	if db == nil {
		return nil, ErrDatabaseConnectionIsNil
	}
	if options.InitializeSchema && isTx(db) {
		return nil, ErrAttemptedTableInitializationWithinTransaction
	}

	ID := uuid.New().String()
	tng := options.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	ctx, cancel := context.WithCancelCause(cmpOrTODO(options.ParentContext, context.Background()))
	return &publisher{
		Context:                   ctx,
		ContextCancel:             cancel,
		InitializeSchema:          options.InitializeSchema,
		UUID:                      ID,
		DB:                        db,
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		Logger: cmpOrTODO[watermill.LoggerAdapter](
			options.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"publisher_id": ID,
		}),
		mu:          sync.Mutex{},
		knownTopics: make(map[string]struct{}),
	}, nil
}

// Publish pushes messages into a topic. Returns [ErrPublisherIsClosed] if the publisher is closed.
func (p *publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.IsClosed() {
		return ErrPublisherIsClosed
	}
	if len(messages) == 0 {
		return nil
	}
	messagesTableName := p.TopicTableNameGenerator(topic)

	ctx, cancel := context.WithTimeout(p.Context, time.Second*15)
	defer cancel()
	if p.InitializeSchema {
		p.mu.Lock()
		if _, ok := p.knownTopics[topic]; !ok {
			if err = createTopicAndOffsetsTablesIfAbsent(
				ctx,
				p.DB,
				messagesTableName,
				p.OffsetsTableNameGenerator(topic),
			); err != nil {
				p.mu.Unlock()
				return err
			}
			p.knownTopics[topic] = struct{}{}
		}
		p.mu.Unlock()
	}

	query := strings.Builder{}
	_, _ = query.WriteString("INSERT INTO '")
	_, _ = query.WriteString(messagesTableName)
	_, _ = query.WriteString("' (uuid, created_at, payload, metadata) VALUES ")

	values := make([]any, 0, len(messages)*4)
	for _, msg := range messages {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return fmt.Errorf("unable to encode message %q metadata to JSON: %w", msg.UUID, err)
		}
		values = append(values, msg.UUID, time.Now().Format(time.RFC3339), msg.Payload, metadata)
		query.WriteString(`(?,?,?,?),`)
	}

	_, err = p.DB.ExecContext(
		ctx,
		strings.TrimRight(query.String(), ","),
		values...,
	)
	// fmt.Println(strings.TrimRight(query.String(), ","))
	return err
}

func (p *publisher) IsClosed() bool {
	select {
	case <-p.Context.Done():
		return true
	default:
		return false
	}
}

func (p *publisher) Close() error {
	if !p.IsClosed() {
		p.ContextCancel(ErrPublisherIsClosed)
	}
	return nil
}

func (p *publisher) String() string {
	return "sqlite3-modernc-publisher-" + p.UUID
}

func isTx(db SQLiteConnection) bool {
	_, dbIsTx := db.(interface {
		Commit() error
		Rollback() error
	})
	return dbIsTx
}
