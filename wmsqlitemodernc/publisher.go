package wmsqlitemodernc

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

type PublisherConfiguration struct {
	Connector                 Connector
	GenerateMessagesTableName TableNameGenerator
	GenerateOffsetsTableName  TableNameGenerator
	Logger                    watermill.LoggerAdapter
}

type publisher struct {
	Context                   context.Context
	UUID                      string
	db                        *sql.DB
	generateMessagesTableName TableNameGenerator
	generateOffsetsTableName  TableNameGenerator
	logger                    watermill.LoggerAdapter // TODO: Implement logging

	mu          sync.Mutex
	knownTopics map[string]struct{}
}

func NewPublisher(ctx context.Context, cfg PublisherConfiguration) (message.Publisher, error) {
	db, err := cfg.Connector.Connect()
	if err != nil {
		return nil, err
	}

	ID := uuid.New().String()
	return &publisher{
		Context: ctx,
		UUID:    ID,
		db:      db,
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
	messagesTableName := p.generateMessagesTableName.GenerateTableName(topic)

	ctx, cancel := context.WithTimeout(p.Context, time.Second*15)
	defer cancel()
	p.mu.Lock()
	if _, ok := p.knownTopics[topic]; !ok {
		if err = createTopicAndOffsetsTablesIfAbsent(
			ctx,
			p.db,
			messagesTableName,
			p.generateOffsetsTableName.GenerateTableName(topic),
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
		// fmt.Println("queued up message for publication:", msg.UUID)
	}

	_, err = p.db.ExecContext(
		ctx,
		strings.TrimRight(query.String(), ","),
		values...,
	)
	return err
}

func (p *publisher) Close() error {
	return p.db.Close()
}

func (p *publisher) String() string {
	return "sqlite3-modernc-publisher" + p.UUID
}
