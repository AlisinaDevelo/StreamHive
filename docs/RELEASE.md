# Release Checklist

Use this checklist for StreamHive `v0.3.0`, the first release with static blob replication, reconnect/backoff, and file-backed storage.

## Preflight

```bash
go test ./...
go test -bench=. -benchmem -run '^$' ./...
P2P_ADDR=127.0.0.1:17070 HEALTH_ADDR=127.0.0.1:18080 make demo-replication
```

## Version

1. Update [internal/version/version.go](../internal/version/version.go) from `0.2.0` to `0.3.0`.
2. Rename the `[Unreleased]` section in [CHANGELOG.md](../CHANGELOG.md) to `[0.3.0] - YYYY-MM-DD`.
3. Commit the version bump:

```bash
git add internal/version/version.go CHANGELOG.md
git commit -m "chore: release v0.3.0"
```

## Tag

```bash
git tag -a v0.3.0 -m "v0.3.0"
git push origin main
git push origin v0.3.0
```

## Release Notes

Highlight:

- Static `blob.put` replication over SHV1 frames.
- CLI demo path with `-replicate`, `-put-key`, `-put-data`, and `-exit-after-put`.
- Durable receiver storage with `-store-dir`.
- Static peer lists plus reconnect/backoff via `-peers` and `-peer-reconnect`.
- `/metrics` counters for replication and transport behavior.
- `FileStore` behind the `storage.BlobStore` interface.

Attach or link the CI SBOM artifact when publishing GitHub release binaries.
