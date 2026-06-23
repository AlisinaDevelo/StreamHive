package replication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/AliSinaDevelo/StreamHive/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeBlobPut(t *testing.T) {
	payload, err := EncodeBlobPut([]byte("alpha"), []byte("hello"), Limits{})
	require.NoError(t, err)

	msg, err := Decode(payload, Limits{})
	require.NoError(t, err)
	assert.Equal(t, MessageTypeBlobPut, msg.Type)
	assert.Equal(t, []byte("alpha"), msg.Key)
	assert.Equal(t, []byte("hello"), msg.Data)
}

func TestApplyBlobPut(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	payload, err := EncodeBlobPut([]byte("k1"), []byte("value"), Limits{})
	require.NoError(t, err)

	require.NoError(t, Apply(ctx, store, payload, Limits{}))
	got, err := store.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
}

func TestApplyRejectsNilStore(t *testing.T) {
	payload, err := EncodeBlobPut([]byte("k1"), []byte("value"), Limits{})
	require.NoError(t, err)

	err = Apply(context.Background(), nil, payload, Limits{})
	assert.ErrorIs(t, err, ErrNilStore)
}

func TestApplyRespectsContextCancellation(t *testing.T) {
	payload, err := EncodeBlobPut([]byte("k1"), []byte("value"), Limits{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = Apply(ctx, storage.NewMemoryStore(), payload, Limits{})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDecodeRejectsUnknownMessageType(t *testing.T) {
	payload, err := json.Marshal(Message{Type: "peer.hello"})
	require.NoError(t, err)

	_, err = Decode(payload, Limits{})
	assert.ErrorIs(t, err, ErrUnknownMessageType)
}

func TestBlobPutValidation(t *testing.T) {
	tests := []struct {
		name   string
		key    []byte
		data   []byte
		limits Limits
		want   error
	}{
		{
			name: "empty key",
			key:  nil,
			want: ErrKeyEmpty,
		},
		{
			name:   "key too large",
			key:    []byte("abcd"),
			limits: Limits{MaxKeyBytes: 3},
			want:   ErrKeyTooLarge,
		},
		{
			name:   "data too large",
			key:    []byte("k"),
			data:   []byte("abcd"),
			limits: Limits{MaxDataBytes: 3},
			want:   ErrDataTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeBlobPut(tt.key, tt.data, tt.limits)
			assert.ErrorIs(t, err, tt.want)
		})
	}
}

func TestDecodeReturnsCopiedSlices(t *testing.T) {
	payload, err := EncodeBlobPut([]byte("alpha"), []byte("hello"), Limits{})
	require.NoError(t, err)

	msg, err := Decode(payload, Limits{})
	require.NoError(t, err)
	msg.Key[0] = 'x'
	msg.Data[0] = 'y'

	again, err := Decode(payload, Limits{})
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha"), again.Key)
	assert.Equal(t, []byte("hello"), again.Data)
}

func TestDecodeInvalidJSON(t *testing.T) {
	_, err := Decode([]byte("{"), Limits{})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrUnknownMessageType))
}
