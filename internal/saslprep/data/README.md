# PostgreSQL SCRAM preparation data

The private `internal/saslprep` package adapts PostgreSQL's SASLprep and NFKC
algorithms. It contains complete tables, not a character subset, and adds no
native library or runtime module dependency.

Pinned reference: PostgreSQL **REL_18_0**. Its
[`unicode_version.h`](https://github.com/postgres/postgres/blob/REL_18_0/src/include/common/unicode_version.h)
specifies Unicode **16.0** for normalization. SASLprep's mapping, prohibited,
unassigned and bidi ranges remain the RFC3454 **Unicode 3.2** tables. These two
versions serve different purposes and must not be conflated.

| Retained source | Upstream location at REL_18_0 | SHA-256 |
| --- | --- | --- |
| `unicode_norm_table.h` | `src/include/common/unicode_norm_table.h` | `610b3c71ba92f8aecd36eec71910fac94e0f286abeb80e188f64c1adb031af6f` |
| `saslprep.c` | `src/common/saslprep.c` | `a47ab6c21ce2a2ae4b8027108bf340d7a714d940d152774d2b9cc3fad31af998` |
| `POSTGRESQL-LICENSE` | `COPYRIGHT` | `5ed3ce5c9373dff7f98b1fae7a6c7ccd98df7d734d46d24c1bcebf1240be8307` |

Sources are available under
[`https://github.com/postgres/postgres/tree/REL_18_0`](https://github.com/postgres/postgres/tree/REL_18_0).
The normalization algorithm reference is
[`src/common/unicode_norm.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/common/unicode_norm.c).
PostgreSQL's copyright and permission notice is retained verbatim in
`POSTGRESQL-LICENSE`. Unicode data is covered by the permission notice retained
in `UNICODE-LICENSE`, downloaded from <https://www.unicode.org/license.txt>.

Regenerate from the repository root:

```text
moon run tools/generate-saslprep.mbtx
moon fmt
```

The generator verifies both source hashes before emitting `tables.mbt`; it also
requires all 6843 decomposition records and 5138 decomposition codepoints.
The source fixtures are data only; no PostgreSQL C source is compiled or linked.

Compatibility details taken from the implementation (rather than its comments):

- Non-ASCII space mapping precedes removal; U+200B therefore becomes U+0020.
- Empty mapped non-ASCII input fails preparation. ASCII input has its own fast path.
- Prohibited/unassigned and bidi checks inspect **mapped input**, not NFKC output.
- Preparation failure returns the **entire original password**. This follows
  PostgreSQL SCRAM's documented fallback; no partial mapping may survive failure.
- The connection accepts MoonBit `String`; an arbitrary non-UTF-8 byte password
  is not an additional API offered by this change.

The small regression suite covers mapping, recursive decomposition, canonical
ordering and blocking, composition exclusions, Hangul, supplementary scalars,
the U+2F868 normalization correction, and PostgreSQL's failure/fallback cases.
Runtime verification is a separate step from source generation and static checks.
