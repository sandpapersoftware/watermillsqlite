package wmsqlitemodernc

import (
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
	destination            chan *message.Message
	logger                 watermill.LoggerAdapter
}

type rawMessage struct {
	Offset   int64
	UUID     string
	Payload  []byte
	Metadata message.Metadata
}

func (s *subscription) nextBatch() (
	lockedOffset int64,
	batch []rawMessage,
	err error,
) {
	// tx, err := s.db.BeginTx(context.TODO(), nil)
	// if err != nil {
	// 	return 0, nil, err
	// }
	// defer func() {
	// 	if err != nil {
	// 		err = errors.Join(err, tx.Rollback())
	// 	}
	// }()

	lock := s.db.QueryRow(s.sqlLockConsumerGroup)
	if err = lock.Err(); err != nil {
		return 0, nil, fmt.Errorf("unable to acquire row lock: %w", err)
	}
	if err = lock.Scan(&lockedOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// row already locked by another consumer
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("unable to scan offset_acked value: %w", err)
	}

	rows, err := s.db.Query(s.sqlNextMessageBatch, lockedOffset)
	if err != nil {
		return 0, nil, err
	}

	rawMetadata := []byte{}
	for rows.Next() {
		next := rawMessage{}
		if err = rows.Scan(&next.Offset, &next.UUID, &next.Payload, &rawMetadata); err != nil {
			return 0, nil, errors.Join(err, rows.Close())
		}
		if err = json.Unmarshal(rawMetadata, &next.Metadata); err != nil {
			return 0, nil, errors.Join(
				fmt.Errorf("unable to parse metadata JSON: %w", err),
				rows.Close(),
			)
		}
		batch = append(batch, next)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, errors.Join(err, rows.Close())
	}
	return lockedOffset, batch, rows.Close()
}

func (s *subscription) Loop(closed <-chan struct{}) {
	// TODO: defer close?
	var (
		lockedOffset    int64
		lastAckedOffset int64
		row             *sql.Row // TODO: remove
		batch           []rawMessage
		err             error
	)

loop:
	for {
		select {
		case <-closed:
			return
		case <-s.pollTicker.C:
		}

		lockedOffset, batch, err = s.nextBatch()
		if err != nil {
			s.logger.Error("next message batch query failed", err, nil)
			continue loop
		}
		lastAckedOffset = lockedOffset

		for _, next := range batch {

		emmitMessage:
			for {
				msg := message.NewMessage(next.UUID, next.Payload)
				msg.Metadata = next.Metadata

				select { // wait for message emission
				case <-closed:
					return
				case s.destination <- msg:
				}

				select { // wait for message acknowledgement
				case <-closed:
					return
				case <-s.pollTicker.C:
					lockedOffset = lastAckedOffset
					row = s.db.QueryRow(s.sqlExtendLock, next.Offset, lastAckedOffset)
					if err = row.Err(); err != nil {
						s.logger.Error("unable to extend lock", err, nil)
						continue loop
					}
				case <-msg.Acked():
					lastAckedOffset = next.Offset
					break emmitMessage
				case <-s.ackChannel():
					// TODO: extend deadline? s.db.QueryRow(s.sqlExtendLock, next.Offset, lockedOffset)
					s.logger.Debug("message took too long to be acknowledged", nil)
					continue emmitMessage // message took too long - retry
				case <-msg.Nacked():
					continue emmitMessage
				}
			}
		}

		if lastAckedOffset > lockedOffset {
			if _, err = s.db.Exec(s.sqlAcknowledgeMessages, lastAckedOffset, lockedOffset); err != nil {
				s.logger.Error("failed to acknowledge processed messages", err, nil)
			}
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
