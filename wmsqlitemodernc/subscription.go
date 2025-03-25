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
	db                     *sql.DB
	pollTicker             *time.Ticker
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

func (s *subscription) nextBatch() (
	batch []rawMessage,
	err error,
) {
	tx, err := s.db.BeginTx(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	lock := tx.QueryRow(s.sqlLockConsumerGroup)
	if err = lock.Err(); err != nil {
		return nil, fmt.Errorf("unable to acquire row lock: %w", err)
	}
	if err = lock.Scan(&s.lockedOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// row already locked by another consumer
			return nil, tx.Rollback()
		}
		return nil, fmt.Errorf("unable to scan offset_acked value: %w", err)
	}

	rows, err := tx.Query(s.sqlNextMessageBatch, s.lockedOffset)
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
	return batch, errors.Join(rows.Close(), tx.Commit())
}

func (s *subscription) Send(closed <-chan struct{}, next rawMessage) error {
	for {
		msg := message.NewMessage(next.UUID, next.Payload)
		msg.Metadata = next.Metadata

		select { // wait for message emission
		case <-closed:
			return nil
		case s.destination <- msg:
		}

		select { // wait for message acknowledgement
		case <-closed:
			return nil
		case <-s.pollTicker.C:
			row := s.db.QueryRow(s.sqlExtendLock, next.Offset, s.lastAckedOffset)
			if err := row.Err(); err != nil {
				return fmt.Errorf("unable to extend lock: %w", err)
			}
			s.lockedOffset = s.lastAckedOffset
		case <-msg.Acked():
			s.lastAckedOffset = next.Offset
			return nil
		case <-s.ackChannel():
			// TODO: extend deadline? s.db.QueryRow(s.sqlExtendLock, next.Offset, lockedOffset)
			s.logger.Debug("message took too long to be acknowledged", nil)
			msg.Nack()
		case <-msg.Nacked():
		}
	}
}

func (s *subscription) Loop(closed <-chan struct{}) {
	// TODO: defer close?
	var (
		batch []rawMessage
		err   error
	)

loop:
	for {
		select {
		case <-closed:
			return
		case <-s.pollTicker.C:
		}

		batch, err = s.nextBatch()
		if err != nil {
			s.logger.Error("next message batch query failed", err, nil)
			continue loop
		}

		for _, next := range batch {
			if err = s.Send(closed, next); err != nil {
				s.logger.Error("failed to process queued message", err, nil)
				continue loop
			}
		}

		if s.lastAckedOffset > s.lockedOffset {
			if _, err = s.db.Exec(
				s.sqlAcknowledgeMessages,
				s.lastAckedOffset,
				s.lockedOffset,
			); err != nil {
				s.logger.Error("failed to acknowledge processed messages", err, nil)
			}
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
