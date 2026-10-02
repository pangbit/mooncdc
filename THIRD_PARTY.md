# Dependencies and references

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
Apache-2.0, with its license retained. They provide real upstream payloads in isolated tests.

Protocol references inform an independent MoonBit implementation. No PostgreSQL client
implementation or other Mooncakes CDC package is copied or adapted. Official
`moonbitlang/async` examples inform the executable example package layout.
