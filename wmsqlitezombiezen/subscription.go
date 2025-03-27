package wmsqlitezombiezen

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type subscription struct {
	Connection   *sqlite.Conn
	pollTicker   *time.Ticker
	lockTicker   *time.Ticker
	lockDuration time.Duration
	ackChannel   func() <-chan time.Time

	stmtLockConsumerGroup   *sqlite.Stmt
	stmtExtendLock          *sqlite.Stmt
	stmtNextMessageBatch    *sqlite.Stmt
	stmtAcknowledgeMessages *sqlite.Stmt

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

func (s *subscription) NextBatch() (batch []rawMessage, err error) {
	closeTransaction, err := sqlitex.ExclusiveTransaction(s.Connection)
	if err != nil {
		return nil, err
	}
	defer closeTransaction(&err)

	defer func() {
		err = errors.Join(err, s.stmtLockConsumerGroup.Reset())
	}()
	ok, err := s.stmtLockConsumerGroup.Step()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset_acked value: %w", err)
	}
	if !ok {
		return nil, sql.ErrNoRows
	}
	s.lockedOffset = s.stmtLockConsumerGroup.ColumnInt64(0)
	s.lastAckedOffset = s.lockedOffset
	ok, err = s.stmtLockConsumerGroup.Step()
	if err != nil {
		return nil, fmt.Errorf("unable to finish reading offset_acked value: %w", err)
	}
	if ok {
		return nil, errors.New("lock query returned more than one row")
	}

	defer func() {
		err = errors.Join(err, s.stmtNextMessageBatch.Reset())
	}()
	s.stmtNextMessageBatch.BindInt64(1, s.lockedOffset)
	for {
		ok, err = s.stmtNextMessageBatch.Step()
		if err != nil {
			return nil, fmt.Errorf("unable to read message row: %w", err)
		}
		if !ok {
			break
		}
		// fmt.Println("offset", s.stmtNextMessageBatch.ColumnText(1))
		next := rawMessage{
			Offset: s.stmtNextMessageBatch.ColumnInt64(0),
			UUID:   s.stmtNextMessageBatch.ColumnText(1),
		}
		b := &bytes.Buffer{} // TODO: use buffer pool as a subscriber option
		b.Reset()            // might be full from pool; note that pool may leak message metadata
		if _, err = io.Copy(b, s.stmtNextMessageBatch.ColumnReader(2)); err != nil {
			return nil, fmt.Errorf("unable to read message payload: %w", err)
		}
		next.Payload = slices.Clone(b.Bytes())
		b.Reset()
		if _, err = io.Copy(b, s.stmtNextMessageBatch.ColumnReader(3)); err != nil {
			return nil, fmt.Errorf("unable to read message metadata: %w", err)
		}

		if err = json.Unmarshal(b.Bytes(), &next.Metadata); err != nil {
			return nil, fmt.Errorf("unable to parse message metadata JSON: %w", err)
		}
		batch = append(batch, next)
	}

	return batch, nil
}

func (s *subscription) ExtendLock() (err error) {
	defer func() {
		err = errors.Join(err, s.stmtExtendLock.Reset())
	}()
	s.stmtExtendLock.BindInt64(1, s.lastAckedOffset)
	s.stmtExtendLock.BindInt64(2, s.lockedOffset)
	if _, err = s.stmtExtendLock.Step(); err != nil {
		return err
	}
	s.lockTicker.Reset(s.lockDuration)
	s.lockedOffset = s.lastAckedOffset
	return nil
}

func (s *subscription) ReleaseLock() (err error) {
	defer func() {
		err = errors.Join(err, s.stmtAcknowledgeMessages.Reset())
	}()
	s.stmtAcknowledgeMessages.BindInt64(1, s.lastAckedOffset)
	s.stmtAcknowledgeMessages.BindInt64(2, s.lockedOffset)
	var ok bool
	for {
		if ok, err = s.stmtAcknowledgeMessages.Step(); err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
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
			return s.ReleaseLock()
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
			if err := s.ExtendLock(); err != nil {
				return err
			}
			goto waitForMessageAcknowledgement
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

func (s *subscription) Run(ctx context.Context) {
	var (
		batch []rawMessage
		err   error
	)

	for {
		select {
		case <-ctx.Done():
			if err = errors.Join(
				ctx.Err(),
				s.stmtLockConsumerGroup.Finalize(),
				s.stmtExtendLock.Finalize(),
				s.stmtNextMessageBatch.Finalize(),
				s.stmtAcknowledgeMessages.Finalize(),
				s.Connection.Close(),
			); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("subscription ended with error", err, nil)
			}
			return
		case <-s.pollTicker.C:
		}

		batch, err = s.NextBatch()
		if err != nil {
			// sql.ErrNoRows indicates failure to acquire consumer group lock
			if !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrNoRows) {
				s.logger.Error("next message batch query failed", err, nil)
			}
			continue
		}

		for _, next := range batch {
			if err = s.Send(ctx, next); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.logger.Error("failed to process queued message", err, nil)
				}
				continue
			}
		}

		if err = s.ReleaseLock(); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logger.Error("failed to acknowledge processed messages", err, nil)
			}
		}
	}
}
