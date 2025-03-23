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
	ticker                 *time.Ticker
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
	metadataParsed := make(map[string]string)
	createdAt := ""
	extendLockTicker := time.NewTicker(time.Second) // TODO: customize
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
		case <-s.ticker.C:
		}

		row = s.db.QueryRow(s.sqlLockConsumerGroup)
		if err = row.Err(); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				s.logger.Error("failed to lock consumer group row", err, nil)
			}
			continue loop
		}
		if err = row.Scan(&lockedOffset); err != nil {
			s.logger.Error("failed to scan consumer group lock offset", err, nil)
			continue loop
		}

		rows, err = s.db.Query(s.sqlNextMessageBatch)
		if err != nil {
			s.logger.Error("failed to query next message batch", err, nil)
			continue loop
		}

		for rows.Next() {
			// "offset", uuid, created_at, payload, metadata
			if err = rows.Scan(&offset, &uuid, &createdAt, &payload, &metadata); err != nil {
				s.logger.Error(
					"failed to scan message",
					errors.Join(err, rows.Close()),
					nil)
				continue loop
			}
			msg := message.NewMessage(uuid, payload)
			msg.Metadata.Set("offset", fmt.Sprintf("%d", offset))
			msg.Metadata.Set("createdAt", createdAt)
			if metadata != nil {
				metadataParsed = make(map[string]string)
				err = json.Unmarshal(metadata, &metadataParsed)
				if err != nil {
					s.logger.Error(
						"failed to unmarshal message metadata",
						errors.Join(err, rows.Close()),
						nil)
					continue loop
				}
				for k, v := range metadataParsed {
					msg.Metadata.Set(k, v)
				}
			}

		emmitMessage:
			for {
				select {
				case <-closed:
					return
				case s.destination <- msg:
					for {
						select {
						case <-extendLockTicker.C:
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
						case <-msg.Nacked():
						}
					}
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
