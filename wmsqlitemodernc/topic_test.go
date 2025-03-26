package wmsqlitemodernc

import (
	"testing"
)

func TestTopicTableCreation(t *testing.T) {
	db := NewGlobalInMemoryEphemeralConnector(t.Context())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	err := createTopicAndOffsetsTablesIfAbsent(
		t.Context(),
		db,
		"messagesTableName",
		"offsetsTableName",
	)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(t.Context(), `
	SELECT
	    name
	FROM
	    sqlite_schema
	WHERE
	    type ='table' AND
	    name NOT LIKE 'sqlite_%';`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
		t.Log("Found table:", name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// t.Fatal(tables)
	if len(tables) != 2 {
		t.Fatal("Expected 2 tables, got", len(tables))
	}
}
