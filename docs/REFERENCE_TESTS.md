# Reference tests and retained contracts

<!-- language-switch -->
[English](REFERENCE_TESTS.md) | [简体中文](REFERENCE_TESTS.zh-CN.md)

English is the authoritative version.


Production code independently implements PostgreSQL's wire/replication formats.
The maintained reference runners compare parsed values and errors with pinned
upstream implementations; they do not copy a client into the production module.

| Runner | Comparison |
| --- | --- |
| `tools/codec-reference-test.mbtx` | Supabase ETL codecs, schema/default-expression vectors |
| `tools/config-reference-test.mbtx` | YAML parser/configuration semantics |
| `tools/protocol-reference-test.mbtx` | pglogrepl v1 vectors |
| `tools/streaming-reference-test.mbtx` | pglogrepl v2 streaming vectors |
| `tools/memory-reference-test.mbtx` | Memory policy arithmetic |
| `tools/memory-sampler-reference-test.mbtx` | Linux memory/cgroup observations |

Versions, archive hashes and fixture inputs are pinned in each runner and under
`integration/reference/`. Reference runners may need Rust/Go and upstream source
caches; they are separate from offline native unit tests. Read the runner before
execution. Do not record new vectors merely to make a comparison pass.

Destination `Accepted` is not a durability acknowledgement. Commit validation
and the contiguous durable barrier must precede checkpoint/feedback advancement.
Snapshot TableEnd is not a transaction commit. Slot invalidation and unsafe
timeline recovery must fail explicitly. These contracts are specified in
[API](API.md) and exercised by the corresponding source test files.

The 2026-10-02 review at `d0172f86c8398cbad3cc239a9504db1e5b6a64b1`
is historical provenance, not a result for the current release tree. See
[validation scope](VALIDATION.md) and [third-party notices](../THIRD_PARTY.md).
