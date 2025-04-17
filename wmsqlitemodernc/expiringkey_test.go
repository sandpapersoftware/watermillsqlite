package wmsqlitemodernc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestExpiringKeyRepository(t *testing.T) {
	// TODO: replace with t.Context() after Watermill bumps to Golang 1.24

	db := newTestConnection(t, "file:"+uuid.New().String()+"?mode=memory&journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
	r, err := NewExpiringKeyRepository(ExpiringKeyRepositoryConfiguration{
		Database: db,
	})
	if err != nil {
		t.Fatal(err)
	}

	isDuplicate, err := r.IsDuplicate(context.TODO(), "test_key")
	if err != nil {
		t.Fatal(err)
	}
	if isDuplicate {
		t.Fatal("key should not be duplicate")
	}
	isDuplicate, err = r.IsDuplicate(context.TODO(), "test_key")
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate {
		t.Fatal("key should be duplicate")
	}

	if err = r.(interface {
		CleanUp(context.Context, time.Time) error
	}).CleanUp(context.TODO(), time.Time{}); err != nil {
		t.Fatal(err)
	}
}
