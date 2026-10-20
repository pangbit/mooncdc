# Memory policy and sampling

Internal building blocks for the Supabase ETL memory governor. The pipeline uses
these samples through its optional pressure and adaptive batch configuration.
This package's exports are internal composition points.

The host and cgroup formulas follow sysinfo 0.39.6, as locked by Supabase ETL
`c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`. Its MIT license is preserved in
`LICENSE-sysinfo`. The domain selection follows ETL's `memory_monitor.rs`.

Linux selects process cgroup limits, then root cgroup limits, then host memory.
Capacity and free headroom are minimized independently across visible ancestors.
The result uses the entire cgroup charge, including file cache, rather than RSS.
A failed refresh of the previously selected cgroup retains the old snapshot.
An unconstrained successful cgroup read selects the host domain.

macOS uses `HW_MEMSIZE` and `HOST_VM_INFO64`: used pages are internal minus
purgeable, plus wired and compressed pages. Each call releases its Mach host port.

When a host read fails, MoonCDC conservatively retains the previous coherent
sample and marks it stale (or returns unknown zero capacity before any success).
This is an explicit failure-policy difference from sysinfo's per-field cache.
Unsupported operating systems also produce a stale unknown sample.

`sample` performs one read. `with_monitor` owns a single periodic task, initializes
pressure from a real sample, skips missed ticks and joins the task on every exit.
Its shared `Monitor` can outlive the scope without retaining the task. Only fresh
capacity changes advance its wrapping revision; usage and source changes do not.

`Governor::with_slots` registers accumulating/in-flight positions for a callback
scope and releases them on success, failure or cancellation. The callback must
include destination drain. Targets refresh from capacity revisions and slot
counts, independently of pressure. These are advisory targets, not reservations.

`Monitor::wait_for_capacity` is a cancellation-aware source gate with hysteresis.
It must be placed before new source reads, never before destination completion.
The pipeline's opt-in emergency pressure option gates its source reads and drains
accepted snapshot rows before waiting. Optional adaptive batches register 17 main
transaction/decoder positions and one per table-copy worker. Snapshot rows stream
into bounded-target batches and drain before accumulating again. Main apply still
owns whole transactions, unlike ETL's two per-event batch positions; it can exceed
the target by an entire transaction. These hints do not enforce a heap limit.
