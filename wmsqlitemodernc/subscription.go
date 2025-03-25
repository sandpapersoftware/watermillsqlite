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
	lockTicker             *time.Ticker
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
	s.lastAckedOffset = s.lockedOffset

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

func (s *subscription) ExtendLock() error {
	// fmt.Sprintf(`UPDATE '%s' SET locked_until=(unixepoch()+%d), offset_acked=? WHERE consumer_group="%s" AND offset_acked=? AND locked_until>=unixepoch() RETURNING COALESCE(locked_until, 0)`, offsetsTableName, graceSeconds, s.consumerGroup)
	row := s.db.QueryRow(s.sqlExtendLock, s.lastAckedOffset, s.lockedOffset)
	if err := row.Err(); err != nil {
		return fmt.Errorf("unable to extend lock: %w", err)
	}
	s.lockedOffset = s.lastAckedOffset
	return nil
}

func (s *subscription) Send(closed <-chan struct{}, next rawMessage) error {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	for {
		msg := message.NewMessage(next.UUID, next.Payload)
		msg.Metadata = next.Metadata
		msg.SetContext(ctx) // required for passing official PubSub test tests.TestMessageCtx

		select { // wait for message emission
		case <-closed:
			return nil
		// TODO: lock should also be extended here if GracePeriod is running out
		case s.destination <- msg:
			// panic("sent")
		}

		// waitForMessageAcknowledgement:
		select {
		case <-closed:
			return nil
		// case <-s.lockTicker.C:
		// 	if err := s.ExtendLock(); err != nil {
		// 		return err
		// 	}
		// 	goto waitForMessageAcknowledgement
		case <-msg.Acked():
			s.lastAckedOffset = next.Offset
			return nil
		case <-s.ackChannel():
			s.logger.Debug("message took too long to be acknowledged", nil)
			msg.Nack()
			if err := s.ExtendLock(); err != nil {
				return err
			}
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

		if _, err = s.db.Exec(
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
