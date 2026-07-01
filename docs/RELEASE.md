# Release Checklist

Use this checklist for StreamHive releases. The current target is `v0.5.0`, the first release with idempotent replication receives, SHA-256 payload verification, periodic anti-entropy sync, and a CI-verified corruption repair demo.

## Preflight

```bash
go test ./...
go test -bench=. -benchmem -run '^$' ./...
P2P_ADDR=127.0.0.1:17070 HEALTH_ADDR=127.0.0.1:18080 make demo-replication
make demo-compose
make demo-repair
go run . -version
```

## Version

1. Update [internal/version/version.go](../internal/version/version.go) to the release semver.
2. Move completed [CHANGELOG.md](../CHANGELOG.md) entries from `[Unreleased]` into `[MAJOR.MINOR.PATCH] - YYYY-MM-DD`.
3. Commit the version bump:

```bash
git add internal/version/version.go CHANGELOG.md
git commit -m "chore: release v0.5.0"
```

## Tag

```bash
git tag -a v0.5.0 -m "v0.5.0"
git push origin main
git push origin v0.5.0
```

## Release Notes

Highlight:

- SHA-256 payload verification for SHA-shaped blob keys.
- Idempotent duplicate `blob.put` handling and duplicate metrics.
- Periodic anti-entropy inventory with `-sync-interval`.
- 3-node corruption repair demo for deleted durable blobs.
- CI coverage for both restart rehydration and corruption repair demos.

Attach or link the CI SBOM artifact when publishing GitHub release binaries.
