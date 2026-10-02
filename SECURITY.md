# Security

For vulnerabilities, use the repository's private vulnerability reporting feature
when available. If unavailable, open an issue requesting a private contact without
including exploit details or secrets. No fixed response SLA is promised.

Use a dedicated replication role, the smallest table/publication permissions and
verified TLS for remote connections. `LocalPlaintext` only accepts numeric loopback
addresses. Do not embed passwords in source code or logs. Test passwords in examples
belong only to the isolated Docker fixtures.

Checkpoint files contain source metadata and positions, not credentials. Keep their
directory private and on a local filesystem with reliable file locking, rename and
fsync semantics. Checksums detect accidental corruption; they do not authenticate
files against a hostile writer.
