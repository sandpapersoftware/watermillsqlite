package wmsqlitezombiezen

import (
	"testing"

	"zombiezen.com/go/sqlite"
)

func newTestConnection(t *testing.T) *sqlite.Conn {
	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		t.Fatal("unable to create test SQLite connetion", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatal("unable to close test SQLite connetion", err)
		}
	})
	conn.SetInterrupt(t.Context().Done())
	return conn
}
