package wmsqlitemodernc

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
	// InitializeSchema is forbidden if using an ongoing transaction as database handle;
	// That could result in an implicit commit of the transaction by a CREATE TABLE statement.
	InitializeSchema bool

	// Logger reports message publishing errors and traces. Defaults value is [watermill.NewSlogLogger].
	Logger watermill.LoggerAdapter
}

type publisher struct {
	Context                   context.Context
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	UUID                      string
	DB                        SQLiteDatabase
	Logger                    watermill.LoggerAdapter

	mu          sync.Mutex
	knownTopics map[string]struct{}
}

// NewPublisher creates a [message.Publisher] instance from a [SQLiteDatabase] connection handler.
func NewPublisher(db SQLiteDatabase, options PublisherOptions) (message.Publisher, error) {
	if db == nil {
		return nil, errors.New("database handle is nil")
	}
	if options.InitializeSchema && isTx(db) {
		// either use a prior schema with a tx db handle, or don't use tx with AutoInitializeSchema
		return nil, errors.New("tried to use AutoInitializeSchema with a database handle that looks like" +
			"an ongoing transaction; this may result in an implicit commit")
	}

	ID := uuid.New().String()
	tng := options.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	return &publisher{
		Context:                   cmp.Or(options.ParentContext, context.Background()),
		UUID:                      ID,
		DB:                        db,
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		Logger: cmp.Or[watermill.LoggerAdapter](
			options.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"publisher_id": ID,
		}),
		mu:          sync.Mutex{},
		knownTopics: make(map[string]struct{}),
	}, nil
}

func (p *publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return nil
	}
	messagesTableName := p.TopicTableNameGenerator(topic)

	ctx, cancel := context.WithTimeout(p.Context, time.Second*15)
	defer cancel()
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
	return err
}

func (p *publisher) Close() error {
	return nil
}

func (p *publisher) String() string {
	return "sqlite3-modernc-publisher-" + p.UUID
}

func isTx(db SQLiteDatabase) bool {
	_, dbIsTx := db.(interface {
		Commit() error
		Rollback() error
	})
	return dbIsTx
}
