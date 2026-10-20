# Pinned Supabase ETL DDL fixture

Unmodified source migrations from Supabase ETL commit
`c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`:

- `crates/etl/migrations/source/20260415100000_schema_change_messages.up.sql`
- `crates/etl/migrations/source/20260724120000_publication_schema_change_messages.up.sql`

Upstream: https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4

Apache-2.0; the upstream LICENSE is retained alongside these fixtures.
Embedded byte-for-byte by `tools/embed-source-migrations.mbtx` for the optional
`run_source_migrations` API and used by isolated compatibility tests. The migrator
creates schema `etl`, validates SQLx history and installs a database-wide event
trigger. Library pipelines opt in with `source_migrations=true`; the standalone
replicator defaults `run_source_migrations` to true, matching the reference.
Test ownership and cleanup must be explicit.
