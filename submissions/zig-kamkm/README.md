# Zig low-level feed server

This submission is a standalone Zig 0.13.0 server. It uses one level-triggered Linux `epoll` loop for both listeners and keeps idle HTTP/1.1 sockets as small connection records; request and response buffers are allocated only while a request is active. A bounded worker pool (default `min(128, 8 * CPU count)`, override with `DB_POOL`, maximum 512) gives each worker one libpq connection, so blocking PostgreSQL round trips never block the socket loop. The workers issue the SQL from `SPEC.md` and format JSON in the process. JWT HS256 checks use OpenSSL HMAC, and the JSON/HTTP parsers are allocation-light.

`install.sh` installs the pinned official Zig 0.13.0 archive and, on Ubuntu, the `libpq-dev` and `libssl-dev` headers used by `@cImport`. `build.sh` links `libpq` and `libcrypto` and emits `feed-zig`; `start.sh` runs it in the foreground. Set the `DATABASE_URL`, `JWT_SECRET`, `HOST`, `PORT`, and optional `EXTRA_PORT` variables from `SPEC.md` before starting.
