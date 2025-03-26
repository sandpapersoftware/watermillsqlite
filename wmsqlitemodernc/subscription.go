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
	db                     DB
	pollTicker             *time.Ticker
	lockDuration           time.Duration
	ackChannel             func() <-chan time.Time
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		switch err {
		case nil:
			err = tx.Commit()
		case ErrConsumerGroupIsLocked:
			// ignore error because operation failed to acquire row lock
			err = tx.Rollback()
		default:
			err = errors.Join(err, tx.Rollback())
		}
	}()

	lock := tx.QueryRowContext(ctx, s.sqlLockConsumerGroup)
	if err = lock.Err(); err != nil {
		return nil, fmt.Errorf("unable to acquire row lock: %w", err)
	}
	if err = lock.Scan(&s.lockedOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConsumerGroupIsLocked
		}
		return nil, fmt.Errorf("unable to scan offset_acked value: %w", err)
	}
	s.lastAckedOffset = s.lockedOffset

	rows, err := tx.QueryContext(ctx, s.sqlNextMessageBatch, s.lockedOffset)
	if err != nil {
		return nil, err
	}

	rawMetadata := []byte{}
	for rows.Next() {
		next := rawMessage{}
		if err = rows.Scan(&next.Offset, &next.UUID, &next.Payload, &rawMetadata); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if err = json.Unmarshal(rawMetadata, &next.Metadata); err != nil {
			return nil, errors.Join(
				fmt.Errorf("unable to parse metadata JSON: %w", err),
				rows.Close(),
			)
		}
		batch = append(batch, next)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	return batch, rows.Close()
}

func (s *subscription) ExtendLock(ctx context.Context) error {
	// fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.consumerGroup)
	row := s.db.QueryRowContext(ctx, s.sqlExtendLock, s.lastAckedOffset, s.lockedOffset)
	if err := row.Err(); err != nil {
		return fmt.Errorf("unable to extend lock: %w", err)
	}
	s.lockedOffset = s.lastAckedOffset
	return nil
}

func (s *subscription) Send(parent context.Context, next rawMessage) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	for {
		msg := message.NewMessage(next.UUID, next.Payload)
		msg.Metadata = next.Metadata
		msg.SetContext(ctx) // required for passing official PubSub test tests.TestMessageCtx

		// s.lockTicker.Reset(d time.Duration)
		select { // wait for message emission
		case <-ctx.Done():
			return nil
		case <-time.After(s.lockDuration):
			return ErrDestinationChannelIsBusy
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
		case <-time.After(s.lockDuration):
			// panic("extend lock")
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

func (s *subscription) Loop(ctx context.Context) {
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
		if err != nil {
			s.logger.Error("next message batch query failed", err, nil)
			continue loop
		}

		for _, next := range batch {
			if err = s.Send(ctx, next); err != nil {
				s.logger.Error("failed to process queued message", err, nil)
				if _, err = s.db.ExecContext(
					ctx,
					s.sqlAcknowledgeMessages,
					s.lastAckedOffset,
					s.lockedOffset,
				); err != nil {
					s.logger.Error("failed to acknowledge processed messages", err, nil)
				}
				<-time.After(time.Second * 5) // let another subscriber work
				continue loop
			}
		}

		if _, err = s.db.ExecContext(
			ctx,
			s.sqlAcknowledgeMessages,
			s.lastAckedOffset,
			s.lockedOffset,
		); err != nil {
			s.logger.Error("failed to acknowledge processed messages", err, nil)
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
