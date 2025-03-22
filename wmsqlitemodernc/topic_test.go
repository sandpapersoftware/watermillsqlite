package wmsqlitemodernc

import (
	"testing"
)

func TestTopicTableCreation(t *testing.T) {
	db, err := NewEphemeralConnector().Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

	})

	err = createTopicAndOffsetsTablesIfAbsent(
		t.Context(),
		db,
		"messagesTableName",
		"offsetsTableName",
	)
	if err != nil {
		t.Fatal(err)
	}
}
