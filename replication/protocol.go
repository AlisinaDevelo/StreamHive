package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AliSinaDevelo/StreamHive/storage"
)

const (
	// MessageTypeBlobPut stores or replaces one blob on a receiving peer.
	MessageTypeBlobPut = "blob.put"

	// DefaultMaxKeyBytes bounds encoded blob keys.
	DefaultMaxKeyBytes = 512
	// DefaultMaxDataBytes bounds replicated blob payloads.
	DefaultMaxDataBytes = 4 << 20
)

var (
	// ErrNilStore is returned when applying a message without a store.
	ErrNilStore = errors.New("replication: nil store")
	// ErrUnknownMessageType is returned for unsupported message types.
	ErrUnknownMessageType = errors.New("replication: unknown message type")
	// ErrKeyEmpty is returned when a blob message has no key.
	ErrKeyEmpty = errors.New("replication: empty key")
	// ErrKeyTooLarge is returned when a key exceeds the configured limit.
	ErrKeyTooLarge = errors.New("replication: key too large")
	// ErrDataTooLarge is returned when a blob exceeds the configured limit.
	ErrDataTooLarge = errors.New("replication: data too large")
)

// Limits bounds decoded message sizes before they are applied.
type Limits struct {
	MaxKeyBytes  int
	MaxDataBytes int
}

// Message is the JSON payload carried inside an SHV1 frame.
type Message struct {
	Type string `json:"type"`
	Key  []byte `json:"key,omitempty"`
	Data []byte `json:"data,omitempty"`
}

// EncodeBlobPut returns a frame payload that stores data under key on receivers.
func EncodeBlobPut(key, data []byte, limits Limits) ([]byte, error) {
	msg := Message{
		Type: MessageTypeBlobPut,
		Key:  append([]byte(nil), key...),
		Data: append([]byte(nil), data...),
	}
	if err := validateBlobPut(msg, normalizeLimits(limits)); err != nil {
		return nil, err
	}
	return json.Marshal(msg)
}

// Decode parses a replication payload and validates its declared message type.
func Decode(payload []byte, limits Limits) (Message, error) {
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return Message{}, fmt.Errorf("replication: decode: %w", err)
	}

	normalized := normalizeLimits(limits)
	switch msg.Type {
	case MessageTypeBlobPut:
		if err := validateBlobPut(msg, normalized); err != nil {
			return Message{}, err
		}
	default:
		return Message{}, ErrUnknownMessageType
	}

	msg.Key = append([]byte(nil), msg.Key...)
	msg.Data = append([]byte(nil), msg.Data...)
	return msg, nil
}

// Apply decodes a replication payload and applies it to store.
func Apply(ctx context.Context, store storage.BlobStore, payload []byte, limits Limits) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return ErrNilStore
	}

	msg, err := Decode(payload, limits)
	if err != nil {
		return err
	}

	switch msg.Type {
	case MessageTypeBlobPut:
		return store.Put(ctx, msg.Key, msg.Data)
	default:
		return ErrUnknownMessageType
	}
}

func normalizeLimits(limits Limits) Limits {
	if limits.MaxKeyBytes <= 0 {
		limits.MaxKeyBytes = DefaultMaxKeyBytes
	}
	if limits.MaxDataBytes <= 0 {
		limits.MaxDataBytes = DefaultMaxDataBytes
	}
	return limits
}

func validateBlobPut(msg Message, limits Limits) error {
	if len(msg.Key) == 0 {
		return ErrKeyEmpty
	}
	if len(msg.Key) > limits.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	if len(msg.Data) > limits.MaxDataBytes {
		return ErrDataTooLarge
	}
	return nil
}
