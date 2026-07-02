# Release Checklist

Use this checklist for StreamHive releases. The current target is `v0.6.0`, the first release with peer visibility endpoints, a Compose cluster status inspector, and a CI-verified reconnect/failure demo.

## Preflight

```bash
go test ./...
go test -bench=. -benchmem -run '^$' ./...
P2P_ADDR=127.0.0.1:17070 HEALTH_ADDR=127.0.0.1:18080 make demo-replication
make demo-compose
make demo-repair
make demo-failure
go run . -version
```

## Version

1. Update [internal/version/version.go](../internal/version/version.go) to the release semver.
2. Move completed [CHANGELOG.md](../CHANGELOG.md) entries from `[Unreleased]` into `[MAJOR.MINOR.PATCH] - YYYY-MM-DD`.
3. Commit the version bump:

```bash
git add internal/version/version.go CHANGELOG.md
git commit -m "chore: release v0.6.0"
```

## Tag

```bash
git tag -a v0.6.0 -m "v0.6.0"
git push origin main
git push origin v0.6.0
```

## Release Notes

Highlight:

- `/peers` JSON endpoint for connected peer visibility.
- `make demo-status` cluster inspector for peers, metrics, and durable keys.
- `make demo-failure` reconnect/failure demo for node restart plus anti-entropy repair.
- CI coverage for restart rehydration, corruption repair, and reconnect/failure demos.

Attach or link the CI SBOM artifact when publishing GitHub release binaries.
