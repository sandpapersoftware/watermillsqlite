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

func (s *subscription) Loop(closed <-chan struct{}) {
	// TODO: defer close?
	lockedOffset := int64(0)
	offset := int64(0)
	uuid := ""
	payload := []byte{}
	metadata := []byte{}
	createdAt := ""
	var (
		row  *sql.Row
		rows *sql.Rows
		err  error
	)

	// 	_, err := db.ExecContext(ctx, `UPDATE '`+offsetsTableName+`' SET locked_until = ? WHERE consumer_group = ?`, time.Now().Add(duration).Unix(), consumerGroup)

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

		rows, err = s.db.Query(s.sqlNextMessageBatch)
		if err != nil {
			s.logger.Error("failed to query next message batch", err, nil)
			continue loop
		}

		for rows.Next() {
			if err = rows.Scan(&offset, &uuid, &createdAt, &payload, &metadata); err != nil {
				s.logger.Error(
					"failed to scan message",
					errors.Join(err, rows.Close()),
					nil)
				continue loop
			}

		emmitMessage:
			for {
				msg := message.NewMessage(uuid, payload)
				msg.Metadata.Set("offset", fmt.Sprintf("%d", offset))
				msg.Metadata.Set("createdAt", createdAt)
				if metadata != nil {
					err = json.Unmarshal(metadata, &msg.Metadata)
					if err != nil {
						s.logger.Error(
							"failed to unmarshal message metadata",
							errors.Join(err, rows.Close()),
							nil)
						continue loop
					}
				}

				select { // wait for message emission
				case <-closed:
					return
				case s.destination <- msg:
				}

				select { // wait for message acknowledgement
				case <-closed:
					return
				case <-s.pollTicker.C:
					row = s.db.QueryRow(s.sqlExtendLock, lockedOffset)
					if err = row.Err(); err != nil {
						s.logger.Error(
							"unable to extend lock",
							errors.Join(err, rows.Close()),
							nil)
						continue loop
					}
				case <-msg.Acked():
					break emmitMessage
				case <-s.ackChannel():
					s.logger.Debug("message took too long to be acknowledged", nil)
					continue emmitMessage // message took too long - retry
				case <-msg.Nacked():
					continue emmitMessage
				}
			}
		}
		if err := rows.Err(); err != nil {
			s.logger.Error(
				"failed to iterate message rows",
				errors.Join(err, rows.Close()),
				nil)
			continue loop
		}
		if err = rows.Close(); err != nil {
			s.logger.Error("failed to close message rows", err, nil)
			continue loop
		}
		if _, err = s.db.Exec(s.sqlAcknowledgeMessages, offset, lockedOffset); err != nil {
			s.logger.Error("failed to acknowledge processed messages", err, nil)
			continue loop
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
