package wmsqlitemodernc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type subscription struct {
	db                    *sql.DB
	ticker                *time.Ticker
	sqlNextMessageBatch   string
	sqlAcknowledgeMessage string
	// acknowledgement        chan bool
	destination chan *message.Message
	logger      watermill.LoggerAdapter
}

func (s *subscription) nextMessageBatch() (*sql.Rows, error) {
	// TODO: customize parent context and query time out duration
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*9)
	// defer cancel()
	return s.db.QueryContext(context.Background(), s.sqlNextMessageBatch)
}

func (s *subscription) emmitMessage(
	closed <-chan struct{},
	msg *message.Message,
) bool {
	for {
		select {
		case <-closed:
			return false
		case s.destination <- msg:
			select {
			// TODO: add time out?
			case <-msg.Acked():
				return true
			case <-msg.Nacked():
			}
		}
	}
}

func (s *subscription) Loop(closed <-chan struct{}) {
	var (
		rows           *sql.Rows
		err            error
		offset         int64
		uuid           string
		payload        []byte
		metadataParsed map[string]string
		metadata       []byte
		createdAt      string
		// createdAtParsed time.Time
		acknowledged bool
	)
	// TODO: defer close

top:
	for {
		select {
		case <-closed:
			return
		case <-s.ticker.C:
		}

		rows, err = s.nextMessageBatch()
		if err != nil {
			// if errors.Is(err, sql.ErrConnDone) {
			// 	panic("connection close")
			// }
			s.logger.Error("failed to fetch next message batch", err, nil)
			continue
		}

		for rows.Next() {
			// offset, uuid, created_at, payload, metadata
			if err := rows.Scan(&offset, &uuid, &createdAt, &payload, &metadata); err != nil {
				s.logger.Error("failed to scan row", errors.Join(err, rows.Close()), nil)
				continue top
			}

			// if createdAtParsed, err = time.Parse(time.RFC3339, createdAt); err != nil {
			// 	s.logger.Error("failed to parse created_at", errors.Join(err, rows.Close()), nil)
			// 	continue top
			// }
			if err = json.Unmarshal(metadata, &metadataParsed); err != nil {
				s.logger.Error("failed to unmarshal metadata", errors.Join(err, rows.Close()), nil)
				continue top
			}

			msg := message.NewMessage(uuid, payload)
			for key, value := range metadataParsed {
				msg.Metadata.Set(key, value)
			}
			// msg.Metadata.Set("timestamp", createdAtParsed.String())
			msg.Metadata.Set("timestamp", createdAt)

			acknowledged = s.emmitMessage(closed, msg)
			if acknowledged {
				if _, err = s.db.Exec(s.sqlAcknowledgeMessage, offset, offset-1); err != nil {
					s.logger.Error("failed to acknowledge message", err, nil)
					<-s.ticker.C
					continue top
				}
				// <-s.ticker.C
				// panic(offset)
			}
		}

		if err := rows.Err(); err != nil {
			s.logger.Error("failed to iterate message rows", err, nil)
			continue
		}
		if err = rows.Close(); err != nil {
			s.logger.Error("failed to close rows", err, nil)
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
