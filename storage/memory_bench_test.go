package storage

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkMemoryStorePut1KB(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	value := make([]byte, 1024)
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		if err := store.Put(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryStoreGet1KB(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	value := make([]byte, 1024)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
		if err := store.Put(ctx, keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := store.Get(ctx, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}
