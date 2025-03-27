package wmsqlitemodernc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type subscription struct {
	DB           SQLiteDatabase
	pollTicker   *time.Ticker
	lockTicker   *time.Ticker
	lockDuration time.Duration
	ackChannel   func() <-chan time.Time

	sqlLockConsumerGroup   string
	sqlExtendLock          string
	sqlNextMessageBatch    string
	sqlAcknowledgeMessages string

	lockedOffset    int64
	lastAckedOffset int64
	destination     chan *message.Message
	logger          watermill.LoggerAdapter
}

type rawMessage struct {
	Offset   int64
	UUID     string
	Payload  []byte
	Metadata message.Metadata
}

func (s *subscription) nextBatch(ctx context.Context) (
	batch []rawMessage,
	err error,
) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	lock := tx.QueryRowContext(ctx, s.sqlLockConsumerGroup)
	if err = lock.Err(); err != nil {
		return nil, fmt.Errorf("unable to acquire row lock: %w", err)
	}
	if err = lock.Scan(&s.lockedOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, tx.Rollback()
		}
		return nil, fmt.Errorf("unable to scan offset_acked value: %w", err)
	}
	s.lastAckedOffset = s.lockedOffset

	rows, err := tx.QueryContext(ctx, s.sqlNextMessageBatch, s.lockedOffset)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(rows.Close())
	}()

	rawMetadata := []byte{}
	for rows.Next() {
		next := rawMessage{}
		if err = rows.Scan(&next.Offset, &next.UUID, &next.Payload, &rawMetadata); err != nil {
			return nil, rows.Close()
		}
		if err = json.Unmarshal(rawMetadata, &next.Metadata); err != nil {
			return nil, fmt.Errorf("unable to parse metadata JSON: %w", err)
		}
		batch = append(batch, next)
	}
	if err := rows.Err(); err != nil {
		return nil, rows.Close()
	}
	return batch, tx.Commit()
}

func (s *subscription) ExtendLock(ctx context.Context) error {
	// fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.consumerGroup)
	row := s.DB.QueryRowContext(ctx, s.sqlExtendLock, s.lastAckedOffset, s.lockedOffset)
	if err := row.Err(); err != nil {
		return fmt.Errorf("unable to extend lock: %w", err)
	}
	s.lockTicker.Reset(s.lockDuration)
	s.lockedOffset = s.lastAckedOffset
	return nil
}

func (s *subscription) ReleaseLock(ctx context.Context) (err error) {
	_, err = s.DB.ExecContext(
		ctx,
		s.sqlAcknowledgeMessages,
		s.lastAckedOffset,
		s.lockedOffset,
	)
	return err
}

func (s *subscription) Send(parent context.Context, next rawMessage) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	s.lockTicker.Reset(s.lockDuration)
	for {
		msg := message.NewMessage(next.UUID, next.Payload)
		msg.Metadata = next.Metadata
		msg.SetContext(ctx) // required for passing official PubSub test tests.TestMessageCtx

		select { // wait for message emission
		case <-ctx.Done():
			return nil
		case <-s.lockTicker.C:
			return s.ReleaseLock(ctx)
		case s.destination <- msg:
		}

	waitForMessageAcknowledgement:
		select {
		case <-ctx.Done():
			msg.Nack()
			return nil
		// TODO: attemped to write a test for this condition
		// it catches double locking even on short time out
		// make sure hungLongAck test behaves properly
		case <-s.lockTicker.C:
			if err := s.ExtendLock(ctx); err != nil {
				return err
			}
			goto waitForMessageAcknowledgement
		case <-msg.Acked():
			s.lastAckedOffset = next.Offset
			return nil
		case <-s.ackChannel():
			s.logger.Debug("message took too long to be acknowledged", nil)
			msg.Nack()
			if err := s.ExtendLock(ctx); err != nil {
				return err
			}
		case <-msg.Nacked():
		}
	}
}

func (s *subscription) Run(ctx context.Context) {
	var (
		batch []rawMessage
		err   error
	)

loop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.pollTicker.C:
		}

		batch, err = s.nextBatch(ctx)
		if err != nil && err != context.Canceled {
			s.logger.Error("next message batch query failed", err, nil)
			continue loop
		}

		for _, next := range batch {
			if err = s.Send(ctx, next); err != nil && err != context.Canceled {
				s.logger.Error("failed to process queued message", err, nil)
				continue loop
			}
		}

		if err = s.ReleaseLock(ctx); err != nil && err != context.Canceled {
			s.logger.Error("failed to acknowledge processed messages", err, nil)
		}
	}
}
