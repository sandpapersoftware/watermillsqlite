package wmsqlitemodernc_test

import (
	"context"
	"testing"
	"time"

	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
)

func TestExpiringKeyRepository(t *testing.T) {
	// TODO: replace with t.Context() after Watermill bumps to Golang 1.24

	db := newTestConnection(t, ":memory:")
	r, err := wmsqlitemodernc.NewExpiringKeyRepository(wmsqlitemodernc.ExpiringKeyRepositoryConfiguration{
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
