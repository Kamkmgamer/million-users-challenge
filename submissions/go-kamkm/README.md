# Go Kamkm feed server

A compact Go 1.27.1 `net/http` server backed by `github.com/jackc/pgx/v5` 5.11.0's native `pgxpool`. It serves both configured ports from one process, keeps HTTP/1.1 connections alive for 180 seconds, and disables TCP keep-alive probes so idle application connections do not generate a packet every few seconds.

## Build and run

`install.sh` installs Go 1.27.1 on a clean Ubuntu host. `build.sh` produces `bin/feed-go-kamkm` with cgo disabled and stripped symbols. `start.sh` runs that binary in the foreground and sets a 75% physical-memory `GOMEMLIMIT` when the operator has not supplied one.

The process consumes `DATABASE_URL`, `JWT_SECRET`, `HOST`, `PORT`, `EXTRA_PORT`, and optional `DB_POOL`. The pool defaults to 32 connections to keep enough I/O in flight without oversubscribing PostgreSQL, and an explicit `DB_POOL` may be 1..512. Each connection starts with `jit=off` and `plan_cache_mode=force_generic_plan`.

## Implementation notes

- Routing is a single allocation-light dispatcher for the five paths. Authentication is checked before the like endpoint parses its post ID.
- JWTs are verified on every authenticated request with a constant-time HMAC-SHA256 comparison, an exact `HS256` algorithm check, and a required unexpired `exp` claim. The `sub` claim must be a positive decimal string.
- Database statements are the queries from `SPEC.md`, issued through one shared pool. Query contexts have a five-second bound, and missing-post likes are translated from the post foreign-key error.
- Authenticated writes enter a bounded 64-item batcher and share one transaction/commit; a failed batch is rolled back and retried individually so per-request errors remain correct.
- Responses use ordered Go structs and UTC millisecond formatting. Gzip response buffers and writers are pooled only for the duration of a response, so idle keep-alive connections do not retain compression state.
- The hot feed response is cached per process for one second and invalidated when a post is created, avoiding a database round trip during read bursts.
