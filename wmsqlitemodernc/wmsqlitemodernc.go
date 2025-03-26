package wmsqlitemodernc

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type DB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Close() error
}

// return New("file:memory:?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared", poolSize)
// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643

type Connector interface {
	Connect() (DB, error)
	GetTopicTableNameGenerator() TableNameGenerator
	GetOffsetsTableNameGenerator() TableNameGenerator
}

type ConnectorConfiguration struct {
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
}

type connector struct {
	DSN                       string
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
}

func (c connector) Connect() (DB, error) {
	return sql.Open("sqlite", c.DSN)
}

func (c connector) GetTopicTableNameGenerator() TableNameGenerator {
	return c.TopicTableNameGenerator
}

func (c connector) GetOffsetsTableNameGenerator() TableNameGenerator {
	return c.OffsetsTableNameGenerator
}

func NewConnector(dsn string, config ConnectorConfiguration) Connector {
	return connector{
		DSN:                       dsn,
		TopicTableNameGenerator:   cmp.Or(config.TopicTableNameGenerator, DefaultMessagesTableNameGenerator),
		OffsetsTableNameGenerator: cmp.Or(config.OffsetsTableNameGenerator, DefaultOffsetsTableNameGenerator),
	}
}

type contextBoundDB struct {
	DB
}

func (c contextBoundDB) Close() error {
	return nil
}

type contextBoundConnector struct {
	DB                        DB
	TopicTableNameGenerator   TableNameGenerator
	OffsetsTableNameGenerator TableNameGenerator
}

func (c contextBoundConnector) Connect() (DB, error) {
	return c.DB, nil
}

func (c contextBoundConnector) GetTopicTableNameGenerator() TableNameGenerator {
	return c.TopicTableNameGenerator
}

func (c contextBoundConnector) GetOffsetsTableNameGenerator() TableNameGenerator {
	return c.OffsetsTableNameGenerator
}

func NewGlobalInMemoryEphemeralConnector(ctx context.Context, config ConnectorConfiguration) Connector {
	db, err := sql.Open("sqlite", "file:memory:?mode=memory&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
	// db, err := sql.Open("sqlite", ":memory:")
	db.SetMaxOpenConns(1)
	if err != nil {
		panic(fmt.Errorf("unable to create RAM emphemeral database connection: %w", err))
	}
	go func(ctx context.Context) {
		<-ctx.Done()
		db.Close()
	}(ctx)

	return &contextBoundConnector{
		DB:                        contextBoundDB{DB: db},
		TopicTableNameGenerator:   cmp.Or(config.TopicTableNameGenerator, DefaultMessagesTableNameGenerator),
		OffsetsTableNameGenerator: cmp.Or(config.OffsetsTableNameGenerator, DefaultOffsetsTableNameGenerator),
	}
}

type TableNameGenerator interface {
	GenerateTableName(topic string) string
}

type TableNameGeneratorFunc func(topic string) string

func (f TableNameGeneratorFunc) GenerateTableName(topic string) string {
	return f(topic)
}

var DefaultMessagesTableNameGenerator TableNameGenerator = TableNameGeneratorFunc(func(topic string) string {
	return "watermill_" + topic
})

var DefaultOffsetsTableNameGenerator TableNameGenerator = TableNameGeneratorFunc(func(topic string) string {
	return "watermill_offsets_" + topic
})
