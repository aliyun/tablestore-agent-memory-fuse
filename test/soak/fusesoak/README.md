# Agent Memory FUSE soak probe

`fusesoak` runs a deterministic mixed-operation workload against an existing
`agent-memory-fuse` mount. Every worker owns distinct paths and repeatedly
performs create, fsync, read/compare, rewrite, append, rename, read/compare,
delete, and absent-path verification. A fixed sentinel per worker is re-read
every 100 successful cycles.

The probe is an acceptance tool, not a load generator. Each worker waits five
seconds after a successful cycle by default, bounding the request volume of a
24-hour stability run. The wait is cancelled promptly when the duration
expires or the process receives `SIGINT` or `SIGTERM`.

## Usage

```sh
go run ./test/soak/fusesoak \
  --root /path/to/mounted/agent-memory-fuse \
  --run-id soak-20260827T120000Z \
  --duration 24h \
  --concurrency 16 \
  --size 1024 \
  --seed 20260827 \
  --cycle-interval 5s
```

`--root` must name an existing directory. `--run-id` is required, must match
`^[A-Za-z0-9_.-]{1,64}$`, and becomes the exact child directory owned by the
run. The probe refuses to reuse a pre-existing run directory. Duration must be
positive, concurrency must be 1 through 256, size must be 1 through 102300
bytes, and cycle interval must be between zero and one minute. A zero interval
is intended only for short, controlled tests.

## Result and cleanup

Exactly one JSON result is written to stdout. It contains UTC start/end time,
requested and elapsed seconds, worker count, payload size, cycle interval,
successful and failed cycle counts, interruption state, a fixed error category,
and a residual path when applicable. It never contains file content, a raw OS
error, endpoint, instance, Store, Scope, or credentials.

A fully successful duration removes only the exact run directory. Any setup,
operation, interruption, or cleanup failure exits nonzero and preserves the
run directory for investigation. A pre-existing run directory is never
removed or modified. Operators must inspect a preserved directory before
performing exact-path cleanup; do not use list-wide or mount-root deletion.

Build a static Linux amd64 binary with:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o fusesoak ./test/soak/fusesoak
```
