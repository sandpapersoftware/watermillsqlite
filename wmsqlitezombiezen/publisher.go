package wmsqlitezombiezen

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// PublisherOptions customize message publishing behavior.
type PublisherOptions struct {
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
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	InitializeSchema          bool
	UUID                      string
	Logger                    watermill.LoggerAdapter

	mu          sync.Mutex
	Closed      bool
	Connection  *sqlite.Conn
	knownTopics map[string]struct{}
}

// NewPublisher creates a [message.Publisher] instance from a [SQLiteDatabase] connection handler.
func NewPublisher(conn *sqlite.Conn, options PublisherOptions) (message.Publisher, error) {
	if conn == nil {
		return nil, ErrDatabaseConnectionIsNil
	}

	ID := uuid.New().String()
	tng := options.TableNameGenerators.WithDefaultGeneratorsInsteadOfNils()
	return &publisher{
		UUID:                      ID,
		TopicTableNameGenerator:   tng.Topic,
		OffsetsTableNameGenerator: tng.Offsets,
		InitializeSchema:          options.InitializeSchema,
		Logger: cmpOrTODO[watermill.LoggerAdapter](
			options.Logger,
			watermill.NewSlogLogger(nil),
		).With(watermill.LogFields{
			"publisher_id": ID,
		}),
		mu:          sync.Mutex{},
		Connection:  conn,
		knownTopics: make(map[string]struct{}),
	}, nil
}

// Publish pushes messages into a topic. Returns [ErrPublisherIsClosed] if the publisher is closed.
func (p *publisher) Publish(topic string, messages ...*message.Message) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Closed {
		return ErrPublisherIsClosed
	}
	if len(messages) == 0 {
		return nil
	}
	messagesTableName := p.TopicTableNameGenerator(topic)

	if p.InitializeSchema {
		if _, ok := p.knownTopics[topic]; !ok {
			if err = createTopicAndOffsetsTablesIfAbsent(
				p.Connection,
				messagesTableName,
				p.OffsetsTableNameGenerator(topic),
			); err != nil {
				p.mu.Unlock()
				return err
			}
			p.knownTopics[topic] = struct{}{}
		}
	}

	query := strings.Builder{}
	_, _ = query.WriteString("INSERT INTO '")
	_, _ = query.WriteString(messagesTableName)
	_, _ = query.WriteString("' (uuid, created_at, payload, metadata) VALUES ")

	arguments := make([]any, 0, len(messages)*4)
	for _, msg := range messages {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return fmt.Errorf("unable to encode message %q metadata to JSON: %w", msg.UUID, err)
		}
		arguments = append(arguments, msg.UUID, time.Now().Format(time.RFC3339), msg.Payload, metadata)
		query.WriteString(`(?,?,?,?),`)
	}

	return sqlitex.ExecuteTransient(
		p.Connection,
		strings.TrimRight(query.String(), ",")+";",
		&sqlitex.ExecOptions{
			Args: arguments,
		})
}

func (p *publisher) Close() error {
	p.mu.Lock()
	p.Closed = true
	p.mu.Unlock()
	return nil
}

func (p *publisher) String() string {
	return "sqlite3-zombiezen-publisher-" + p.UUID
}
