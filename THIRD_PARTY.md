# Dependencies and references

<!-- language-switch -->
[English](THIRD_PARTY.md) | [简体中文](THIRD_PARTY.zh-CN.md)

English is the authoritative version.


- [moonbitlang/core](https://github.com/moonbitlang/core), Apache-2.0:
  standard collections, binary encoding and JSON.
- [moonbitlang/async](https://github.com/moonbitlang/async), Apache-2.0:
  native TCP, structured cancellation, TLS and filesystem I/O. TLS uses the
  platform OpenSSL runtime on macOS/Linux; no OpenSSL sources are vendored here.
- [moonbitlang/x](https://github.com/moonbitlang/x), Apache-2.0:
  SHA-256 and HMAC primitives. Versions are recorded in `moon.mod`.
- [PostgreSQL streaming replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html).
- [PostgreSQL logical message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).
- [PostgreSQL SASL authentication](https://www.postgresql.org/docs/18/sasl-authentication.html).
- [RFC 7677 SCRAM-SHA-256](https://www.rfc-editor.org/rfc/rfc7677).

`integration/reference/etl/` contains two unmodified source DDL migrations from
[Supabase ETL](https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4),
Apache-2.0, with its license retained. They provide real upstream payloads in isolated tests
and are embedded unchanged in `source_migrations_sql.mbt` by the checked-in `.mbtx`
generator. The optional migrator follows SQLx 0.9.0's PostgreSQL lock/history/checksum
format; its implementation is independently written in MoonBit.

`tools/codec-reference-test.mbtx` downloads the same frozen ETL commit, verifies
the source archive hash and builds its unmodified codec with the upstream lockfile.
`integration/reference/codec_driver.rs` and `schema_driver.rs` are our comparison
fixtures; they call the upstream codec/planner without copying those implementations.
The production float converter uses the platform C runtime with a private C locale.

## Implementation provenance

The SCRAM password preparation in `internal/saslprep/` adapts PostgreSQL 18.0's
SASLprep and NFKC algorithms, with its complete generated Unicode normalization and
RFC3454 range tables. It adds no runtime dependency. The pinned sources, SHA-256
hashes, PostgreSQL license and Unicode License V3 are retained in
[`internal/saslprep/data/`](internal/saslprep/data/README.md).

Other protocol references inform an independent MoonBit implementation. No other
PostgreSQL client implementation or Mooncakes CDC package is copied or adapted. Official
`moonbitlang/async` examples inform the executable example package layout.
