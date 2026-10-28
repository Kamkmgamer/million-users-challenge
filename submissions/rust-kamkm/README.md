# rust-luna

Rust feed API implementation for `SPEC.md`.

## Stack

- Rust 1.94.0, Tokio multi-thread runtime
- Hyper HTTP/1.1 with one task per keep-alive connection
- `deadpool-postgres`/`tokio-postgres` with the required SQL
- `serde_json` compact JSON and `jsonwebtoken` HS256 validation
- gzip responses when the request advertises `Accept-Encoding: gzip`

The service binds `HOST:PORT` and `HOST:EXTRA_PORT` (when `EXTRA_PORT` is set) in one process. Listeners use a
65,535 connection backlog and accepted sockets have `TCP_NODELAY` enabled. `start.sh` raises the soft file
descriptor limit to the hard limit so idle keep-alive connections are not capped by an interactive shell's default
limit.

The Postgres pool defaults to `min(128, 8 * available_parallelism())`; set `DB_POOL` (1 through 512) to override it. Waiting for a
pool slot is bounded at five seconds and becomes the API's 500 response, except a failed `/health` query, which
returns the specified 503 degraded response.

## Build and run

```sh
./install.sh
./build.sh
DATABASE_URL=postgres://feed:password@db:5432/feed \
JWT_SECRET=million-users-challenge HOST=0.0.0.0 PORT=80 EXTRA_PORT=81 ./start.sh
```

`cargo build --release --locked` uses the checked-in lockfile and `-C target-cpu=native` on the benchmark machines.
The binary itself links only the system C runtime; the Postgres driver uses `NoTls` for the private database network.

Run the repository's validation suite against a fresh `feed_seed` clone with:

```sh
bash test/run.sh submissions/rust-luna
```

