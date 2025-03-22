package wmsqlitemodernc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

func (s *subscription) nextMessageBatch(ctx context.Context) (result []*message.Message, err error) {
	// TODO: customize parent context and query time out duration
	ctx, cancel := context.WithTimeout(ctx, time.Second*9)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.sqlNextMessageBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to query next message batch: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	for rows.Next() {
		offset := int64(0)
		uuid := ""
		payload := []byte{}
		metadata := []byte{}
		metadataParsed := make(map[string]string)
		createdAt := ""

		// "offset", uuid, created_at, payload, metadata
		err = rows.Scan(&offset, &uuid, &createdAt, &payload, &metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to scan message row: %w", err)
		}
		msg := message.NewMessage(uuid, payload)
		msg.Metadata.Set("offset", fmt.Sprintf("%d", offset))
		msg.Metadata.Set("createdAt", createdAt)
		if metadata != nil {
			err = json.Unmarshal(metadata, &metadataParsed)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
			for k, v := range metadataParsed {
				msg.Metadata.Set(k, v)
			}
		}
		result = append(result, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate message rows: %w", err)
	}

	return
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
	// TODO: defer close

top:
	for {
		select {
		case <-closed:
			return
		case <-s.ticker.C:
		}

		msgs, err := s.nextMessageBatch(context.Background())
		if err != nil {
			// if errors.Is(err, sql.ErrConnDone) {
			// 	panic("connection close")
			// }
			s.logger.Error("failed to fetch next message batch", err, nil)
			continue
		}
		for _, msg := range msgs {
			offset, err := strconv.ParseInt(msg.Metadata.Get("offset"), 10, 64)
			if err != nil {
				s.logger.Error("failed to parse offset", err, nil)
				continue
			}
			if s.emmitMessage(closed, msg) {
				if _, err = s.db.Exec(s.sqlAcknowledgeMessage, offset, offset-1); err != nil {
					s.logger.Error("failed to acknowledge message", err, nil)
					// panic(err)
					// <-s.ticker.C
					continue top
				}
				// <-s.ticker.C
				// panic(offset)
			}
		}
	}
}

func (s *subscription) Close() error {
	return s.db.Close()
}
