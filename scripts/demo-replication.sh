#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
BIN="$ROOT_DIR/bin/fs"
P2P_ADDR="${P2P_ADDR:-127.0.0.1:7070}"
HEALTH_ADDR="${HEALTH_ADDR:-127.0.0.1:8080}"
BLOB_KEY="${BLOB_KEY:-demo}"
BLOB_DATA="${BLOB_DATA:-hello streamhive}"
LOG_FILE="$(mktemp -t streamhive-demo.XXXXXX)"

receiver_pid=""
cleanup() {
	if [ -n "$receiver_pid" ]; then
		kill "$receiver_pid" 2>/dev/null || true
		wait "$receiver_pid" 2>/dev/null || true
	fi
	rm -f "$LOG_FILE"
}
trap cleanup EXIT INT TERM

mkdir -p "$ROOT_DIR/bin"
go build -o "$BIN" "$ROOT_DIR"

"$BIN" -listen "$P2P_ADDR" -replicate -health "$HEALTH_ADDR" >"$LOG_FILE" 2>&1 &
receiver_pid=$!

i=0
until curl -fsS "http://$HEALTH_ADDR/readyz" >/dev/null 2>&1; do
	i=$((i + 1))
	if [ "$i" -gt 50 ]; then
		echo "receiver did not become ready" >&2
		cat "$LOG_FILE" >&2
		exit 1
	fi
	sleep 0.1
done

"$BIN" \
	-listen 127.0.0.1:0 \
	-dial "$P2P_ADDR" \
	-put-key "$BLOB_KEY" \
	-put-data "$BLOB_DATA" \
	-exit-after-put

i=0
until curl -fsS "http://$HEALTH_ADDR/metrics" | grep '"replication_blobs_stored": 1' >/dev/null; do
	i=$((i + 1))
	if [ "$i" -gt 50 ]; then
		echo "receiver did not store replicated blob" >&2
		cat "$LOG_FILE" >&2
		exit 1
	fi
	sleep 0.1
done

echo "replicated blob '$BLOB_KEY' (${#BLOB_DATA} bytes)"
curl -fsS "http://$HEALTH_ADDR/metrics"
echo
