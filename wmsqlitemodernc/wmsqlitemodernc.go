package wmsqlitemodernc

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// return New("file:memory:?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared", poolSize)
// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643

type Connector interface {
	Connect() (*sql.DB, error)
}

type ConnectorFunc func() (*sql.DB, error)

func (f ConnectorFunc) Connect() (*sql.DB, error) {
	return f()
}

func NewConnector(dsn string) Connector {
	return ConnectorFunc(func() (*sql.DB, error) {
		return sql.Open("sqlite", dsn)
	})
}

func NewEphemeralConnector() Connector {
	db, err := sql.Open("sqlite", "file:memory:?mode=memory&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
	// db.SetMaxOpenConns(1) // CRITICAL
	if err != nil {
		// panic(err)
		err = fmt.Errorf("unable to create RAM emphemeral database connection: %w", err)
		return ConnectorFunc(func() (*sql.DB, error) {
			return nil, err
		})
	}

	return ConnectorFunc(func() (*sql.DB, error) {
		return db, nil
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
