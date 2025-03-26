package wmsqlitemodernc

import (
	"cmp"
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

type PublisherConfiguration struct {
	Connector Connector
	Logger    watermill.LoggerAdapter
}

type publisher struct {
	Context                   context.Context
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
	UUID                      string
	DB                        DB
	Logger                    watermill.LoggerAdapter

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
		Context:                   ctx,
		UUID:                      ID,
		DB:                        db,
		TopicTableNameGenerator:   cfg.Connector.GetTopicTableNameGenerator(),
		OffsetsTableNameGenerator: cfg.Connector.GetOffsetsTableNameGenerator(),
		Logger: cmp.Or[watermill.LoggerAdapter](
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
	messagesTableName := p.TopicTableNameGenerator.GenerateTableName(topic)

	ctx, cancel := context.WithTimeout(p.Context, time.Second*15)
	defer cancel()
	p.mu.Lock()
	if _, ok := p.knownTopics[topic]; !ok {
		if err = createTopicAndOffsetsTablesIfAbsent(
			ctx,
			p.DB,
			messagesTableName,
			p.OffsetsTableNameGenerator.GenerateTableName(topic),
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
	return p.DB.Close()
}

func (p *publisher) String() string {
	return "sqlite3-modernc-publisher-" + p.UUID
}
