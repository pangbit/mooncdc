# Pinned Supabase ETL DDL fixture

Unmodified source migrations from Supabase ETL commit
`c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`:

- `crates/etl/migrations/source/20260415100000_schema_change_messages.up.sql`
- `crates/etl/migrations/source/20260724120000_publication_schema_change_messages.up.sql`

Upstream: https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4

Apache-2.0; the upstream LICENSE is retained alongside these fixtures.
Used only by the isolated PostgreSQL compatibility test. These migrations require an
`etl` schema and install a database-wide event trigger. The library never installs them
automatically. Test ownership and cleanup must be explicit.
