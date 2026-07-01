# Release Checklist

Use this checklist for StreamHive releases. The current target is `v0.4.0`, the first release with content-addressed CLI puts, startup anti-entropy sync, Prometheus text metrics, and a CI-verified 3-node Compose rehydration demo.

## Preflight

```bash
go test ./...
go test -bench=. -benchmem -run '^$' ./...
P2P_ADDR=127.0.0.1:17070 HEALTH_ADDR=127.0.0.1:18080 make demo-replication
make demo-compose
go run . -version
```

## Version

1. Update [internal/version/version.go](../internal/version/version.go) to the release semver.
2. Move completed [CHANGELOG.md](../CHANGELOG.md) entries from `[Unreleased]` into `[MAJOR.MINOR.PATCH] - YYYY-MM-DD`.
3. Commit the version bump:

```bash
git add internal/version/version.go CHANGELOG.md
git commit -m "chore: release v0.4.0"
```

## Tag

```bash
git tag -a v0.4.0 -m "v0.4.0"
git push origin main
git push origin v0.4.0
```

## Release Notes

Highlight:

- SHA-256 content key helpers and `-put-content-key`.
- `blob.has`, `blob.get`, and `blob.missing` anti-entropy messages.
- Startup anti-entropy sync for static `-replicate` peers.
- Durable receiver storage with `-store-dir` and `-list-keys`.
- `/metrics` JSON counters and `/metrics/prometheus` text format.
- 3-node Docker Compose demo with node restart rehydration, verified in CI.

Attach or link the CI SBOM artifact when publishing GitHub release binaries.
