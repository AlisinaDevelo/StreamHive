package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSHA256Key(t *testing.T) {
	data := []byte("hello streamhive")
	wantHex := "aa341580fa480f9b9a7b0f05be16bf751bd19a858ee6ad468212fb78531c9c2e"

	key := SHA256Key(data)
	require.Len(t, key, SHA256KeyBytes)
	assert.Equal(t, wantHex, SHA256KeyHex(data))

	parsed, err := ParseSHA256KeyHex(wantHex)
	require.NoError(t, err)
	assert.Equal(t, key, parsed)
}

func TestSHA256KeyReturnsCopy(t *testing.T) {
	a := SHA256Key([]byte("same"))
	b := SHA256Key([]byte("same"))
	a[0] ^= 0xff
	assert.NotEqual(t, a, b)
}

func TestParseSHA256KeyHexRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "short", key: "abc"},
		{name: "bad hex", key: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSHA256KeyHex(tt.key)
			assert.ErrorIs(t, err, ErrInvalidSHA256HexKey)
		})
	}
}

func TestValidateSHA256Key(t *testing.T) {
	assert.NoError(t, ValidateSHA256Key(SHA256Key([]byte("ok"))))
	err := ValidateSHA256Key([]byte("short"))
	assert.True(t, errors.Is(err, ErrInvalidSHA256Key))
}
