package storage

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStore_PutGetRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	require.NoError(t, err)

	key := []byte("alpha")
	require.NoError(t, store.Put(ctx, key, []byte("hello")))

	reopened, err := NewFileStore(dir)
	require.NoError(t, err)
	got, err := reopened.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
}

func TestFileStore_HexEncodesKeys(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	require.NoError(t, err)

	key := []byte("../escape")
	require.NoError(t, store.Put(ctx, key, []byte("safe")))

	encoded := hex.EncodeToString(key)
	data, err := os.ReadFile(filepath.Join(dir, encoded))
	require.NoError(t, err)
	assert.Equal(t, []byte("safe"), data)
	assert.NoFileExists(t, filepath.Join(dir, "..", "escape"))
}

func TestFileStore_PutReplace(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	key := []byte("k")
	require.NoError(t, store.Put(ctx, key, []byte("a")))
	require.NoError(t, store.Put(ctx, key, []byte("b")))

	got, err := store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), got)
}

func TestFileStore_NotFoundAndHas(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	has, err := store.Has(ctx, []byte("missing"))
	require.NoError(t, err)
	assert.False(t, has)

	_, err = store.Get(ctx, []byte("missing"))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestFileStore_Delete(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	key := []byte("k")
	require.NoError(t, store.Put(ctx, key, []byte("value")))
	require.NoError(t, store.Delete(ctx, key))
	require.NoError(t, store.Delete(ctx, key))

	has, err := store.Has(ctx, key)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestFileStore_EmptyKey(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	assert.ErrorIs(t, store.Put(ctx, nil, []byte("value")), ErrKeyEmpty)
	assert.ErrorIs(t, store.Delete(ctx, nil), ErrKeyEmpty)
}

func TestFileStore_ContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	assert.Error(t, store.Put(ctx, []byte("k"), []byte("value")))
	_, err = store.Get(ctx, []byte("k"))
	assert.Error(t, err)
}

func TestNewFileStore_EmptyDirectory(t *testing.T) {
	_, err := NewFileStore("")
	require.Error(t, err)
}
