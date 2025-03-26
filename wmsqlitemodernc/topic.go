package wmsqlitemodernc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
)

var disallowedTopicCharacters = regexp.MustCompile(`[^A-Za-z0-9\-\$\:\.\_]`)

var ErrInvalidTopicName = errors.New("topic name should not contain characters matched by " + disallowedTopicCharacters.String())

// validateTopicName checks if the topic name contains any characters which could be unsuitable for the SQL Pub/Sub.
// Topics are translated into SQL tables and patched into some queries, so this is done to prevent injection as well.
func validateTopicName(topic string) error {
	if disallowedTopicCharacters.MatchString(topic) {
		return fmt.Errorf("%s: %w", topic, ErrInvalidTopicName)
	}
	return nil
}

func createTopicAndOffsetsTablesIfAbsent(ctx context.Context, db DB, messagesTableName, offsetsTableName string) (err error) {
	if err = validateTopicName(messagesTableName); err != nil {
		return err
	}
	// TODO: transaction had a bug, because db was used instead of tx, when fixed the acceptance tests began to fail
	// tx, err := db.BeginTx(ctx, nil)
	// if err != nil {
	// 	return err
	// }
	// defer func() {
	// 	if err != nil {
	// 		err = errors.Join(err, tx.Rollback())
	// 	}
	// }()

	// Adding UNIQUE(uuid) constraint slows the driver
	// down without benefit? Also, it will fail
	// the official `TestMessageCtx` acceptance test,
	// which attempts to send the exact same message twice.
	// But removing it will fail the bulk insertion test.
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS '`+messagesTableName+`' (
		'offset' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		uuid TEXT NOT NULL,
		created_at TEXT NOT NULL,
		payload BLOB NOT NULL,
		metadata JSON NOT NULL
	);`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS '`+offsetsTableName+`' (
		consumer_group TEXT NOT NULL,
		offset_acked INTEGER NOT NULL,
		locked_until INTEGER NOT NULL,
		PRIMARY KEY(consumer_group)
	);`)
	if err != nil {
		return err
	}
	// return tx.Commit()
	return err
}
