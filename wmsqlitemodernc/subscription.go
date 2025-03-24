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

func (s *subscription) nextBatch() (result []rawMessage, err error) {
	rows, err := s.db.Query(s.sqlNextMessageBatch)
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
		result = append(result, next)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	return result, rows.Close()
}

func (s *subscription) Loop(closed <-chan struct{}) {
	// TODO: defer close?
	var (
		lockedOffset    int64
		lastAckedOffset int64
		row             *sql.Row
		err             error
	)

loop:
	for {
		select {
		case <-closed:
			return
		case <-s.pollTicker.C:
		}

		row = s.db.QueryRow(s.sqlLockConsumerGroup)
		if err = row.Err(); err != nil {
			s.logger.Error("failed to lock consumer group row", err, nil)
			continue loop
		}
		if err = row.Scan(&lockedOffset); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				s.logger.Error("failed to scan consumer group lock offset", err, nil)
			}
			continue loop
		}
		lastAckedOffset = lockedOffset

		batch, err := s.nextBatch()
		if err != nil {
			s.logger.Error("next message batch query failed", err, nil)
			continue loop
		}

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
		if _, err = s.db.Exec(s.sqlAcknowledgeMessages, lastAckedOffset, lockedOffset); err != nil {
			s.logger.Error("failed to acknowledge processed messages", err, nil)
			continue loop
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
