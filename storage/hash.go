package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const (
	// SHA256KeyBytes is the byte length of a SHA-256 content key.
	SHA256KeyBytes = sha256.Size
	// SHA256KeyHexBytes is the encoded length of a SHA-256 content key.
	SHA256KeyHexBytes = sha256.Size * 2
)

var (
	// ErrInvalidSHA256Key is returned when a key is not a SHA-256 digest.
	ErrInvalidSHA256Key = errors.New("storage: invalid sha256 key")
	// ErrInvalidSHA256HexKey is returned when a key is not a hex-encoded SHA-256 digest.
	ErrInvalidSHA256HexKey = errors.New("storage: invalid sha256 hex key")
)

// SHA256Key returns the content-addressed key for data.
func SHA256Key(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// SHA256KeyHex returns the hex-encoded content-addressed key for data.
func SHA256KeyHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ParseSHA256KeyHex parses a hex-encoded SHA-256 key.
func ParseSHA256KeyHex(s string) ([]byte, error) {
	if len(s) != SHA256KeyHexBytes {
		return nil, ErrInvalidSHA256HexKey
	}
	key, err := hex.DecodeString(s)
	if err != nil {
		return nil, ErrInvalidSHA256HexKey
	}
	if err := ValidateSHA256Key(key); err != nil {
		return nil, err
	}
	return key, nil
}

// ValidateSHA256Key verifies that key has the byte length of a SHA-256 digest.
func ValidateSHA256Key(key []byte) error {
	if len(key) != SHA256KeyBytes {
		return ErrInvalidSHA256Key
	}
	return nil
}
