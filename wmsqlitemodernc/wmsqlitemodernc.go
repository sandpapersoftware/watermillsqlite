package wmsqlitemodernc

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type SQLiteDatabase interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Close() error
}

// return New("file:memory:?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared", poolSize)
// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643

func NewGlobalInMemoryEphemeralConnector(ctx context.Context) SQLiteDatabase {
	db, err := sql.Open("sqlite", "file:memory:?mode=memory&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared&_txlock=exclusive")
	// db, err := sql.Open("sqlite", ":memory:")
	db.SetMaxOpenConns(1)
	if err != nil {
		panic(fmt.Errorf("unable to create RAM emphemeral database connection: %w", err))
	}
	go func(ctx context.Context) {
		<-ctx.Done()
		db.Close()
	}(ctx)

	return db
}

// TableNameGenerator creates a table name for a given topic either for
// a topic table or for offsets table.
type TableNameGenerator func(topic string) string

// TableNameGenerators is a struct that holds two functions for generating topic and offsets table names.
// A [Publisher] and a [Subscriber] must use identical generators for topic and offsets tables in order
// to communicate with each other.
type TableNameGenerators struct {
	Topic   TableNameGenerator
	Offsets TableNameGenerator
}

// WithDefaultGeneratorsInsteadOfNils returns a TableNameGenerators with default generators for topic and offsets tables
// if they were left nil.
func (t TableNameGenerators) WithDefaultGeneratorsInsteadOfNils() TableNameGenerators {
	if t.Topic == nil {
		t.Topic = func(topic string) string {
			return "watermill_" + topic
		}
	}
	if t.Offsets == nil {
		t.Offsets = func(topic string) string {
			return "watermill_offsets_" + topic
		}
	}
	return t
}
