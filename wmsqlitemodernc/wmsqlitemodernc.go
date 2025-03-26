package wmsqlitemodernc

import (
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
}

type ConnectorFunc func() (DB, error)

func (f ConnectorFunc) Connect() (DB, error) {
	return f()
}

func NewConnector(dsn string) Connector {
	return ConnectorFunc(func() (DB, error) {
		return sql.Open("sqlite", dsn)
	})
}

type contextBoundDB struct {
	DB
}

func NewContextBoundDB(ctx context.Context, db DB) DB {
	go func(ctx context.Context) {
		<-ctx.Done()
		db.Close()
	}(ctx)
	return &contextBoundDB{DB: db}
}

func (c *contextBoundDB) Close() error {
	return nil
}

func NewGlobalInMemoryEphemeralConnector(ctx context.Context) Connector {
	db, err := sql.Open("sqlite", "file:memory:?mode=memory&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
	// db, err := sql.Open("sqlite", ":memory:")
	db.SetMaxOpenConns(1)
	if err != nil {
		// panic(err)
		err = fmt.Errorf("unable to create RAM emphemeral database connection: %w", err)
		return ConnectorFunc(func() (DB, error) {
			return nil, err
		})
	}

	return ConnectorFunc(func() (DB, error) {
		return NewContextBoundDB(ctx, db), nil
	})
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
