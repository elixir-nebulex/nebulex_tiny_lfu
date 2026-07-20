# Architecture

This guide describes the internal architecture of the `nebulex_tiny_lfu` adapter,
which implements the **Window TinyLFU (W-TinyLFU)** cache admission policy. The
design closely follows [Caffeine](https://github.com/ben-manes/caffeine) (Java),
adapted for the BEAM's concurrency model.

## Overview

W-TinyLFU combines **recency** and **frequency** to achieve near-optimal cache
hit rates. Unlike simple LRU caches that only track recency, W-TinyLFU uses a
frequency sketch to filter out entries that are unlikely to be accessed again,
keeping the most valuable entries in the cache.

The key insight is that most real-world workloads have a skewed access pattern
(some keys are accessed far more frequently than others), and a frequency-aware
policy can exploit this to significantly outperform LRU.

## Cache Segments

The cache is divided into two regions:

```ascii
                          Total Cache Capacity
  ┌──────────┬──────────────────────────────────────────────┐
  │  Window  │                    Main                      │
  │   (~1%)  │                   (~99%)                     │
  │   LRU    │  ┌─────────────────┬──────────────────────┐  │
  │          │  │   Probation     │      Protected       │  │
  │          │  │  (~20% of main) │   (~80% of main)     │  │
  │          │  │     LRU         │       LRU            │  │
  │          │  └─────────────────┴──────────────────────┘  │
  └──────────┴──────────────────────────────────────────────┘
```

- **Window Cache (~1%)**: A small LRU region with no admission filter. Every new
  entry enters here first. This absorbs burst/scan traffic without polluting the
  main cache.

- **Main Cache (~99%)**: A Segmented LRU (SLRU) protected by the TinyLFU
  admission filter:
  - **Probation (~20% of main)**: Entries admitted from the Window. These are
    "on probation" — they'll be evicted first unless accessed again.
  - **Protected (~80% of main)**: Entries promoted from Probation on access.
    These are the most valuable entries in the cache.

The segment sizes follow Caffeine's defaults from `BoundedLocalCache.java`:
`PERCENT_MAIN = 0.99` and `PERCENT_MAIN_PROTECTED = 0.80`.

## Core Components

```ascii
  ┌─────────────────────────────────────────────────────────┐
  │  Nebulex.TinyLFU.Supervisor (:rest_for_one)             │
  │                                                         │
  │  ├── AccessOrderDeque  (Window)                         │
  │  ├── AccessOrderDeque  (Probation)                      │
  │  ├── AccessOrderDeque  (Protected)                      │
  │  └── Maintenance.Supervisor (:rest_for_one)             │
  │        ├── Maintenance Queue  (Tidefall.Queue)          │
  │        ├── Read Buffer        (Tidefall.HashMap)        │
  │        └── Write Buffer       (Tidefall.HashMap)        │
  └─────────────────────────────────────────────────────────┘
```

### FrequencySketch

A probabilistic data structure (4-bit Count-Min Sketch) that estimates how
frequently a key has been accessed. Used by the admission policy to decide
whether a new entry is worth keeping.

- Maximum frequency per key: **15** (4-bit counters)
- Depth: **4 hash functions** (93.75% confidence)
- Memory: ~8 bytes per cache entry
- Periodic aging: halves all counters when the sample window is reached,
  keeping the sketch fresh and responsive to changing access patterns

See `Nebulex.TinyLFU.FrequencySketch`.

### AccessOrderDeque

An LRU-ordered deque backed by two ETS tables. Maintains access ordering within
each cache segment (Window, Probation, Protected).

- **Data table** (`:set`): `key -> order_key` for O(1) membership checks
- **Order table** (`:ordered_set`): `order_key -> key` for sorted access order

Provides O(1) eviction of the LRU entry and O(log n) insertion and touch
operations. Three instances are used — one per segment.

See `Nebulex.TinyLFU.AccessOrderDeque`.

### Buffer Pipeline

A 3-tier pipeline that decouples the hot path (cache reads/writes) from the
cold path (maintenance/eviction):

1. **Read Buffer** (`Tidefall.HashMap`): Captures read events with
   deduplication. Multiple partitions for scalability. Lossy — dropping a
   read event only means one missed LRU touch.

2. **Write Buffer** (`Tidefall.HashMap`): Captures write and delete
   events with deduplication. Lossless — write events cannot be dropped as
   they affect size accounting and eviction.

3. **Maintenance Queue** (`Tidefall.Queue`, 1 partition): Ordered
   queue that serializes all maintenance work. A single processor drains
   events and updates the deques, running eviction and admission as needed.

See `Nebulex.TinyLFU.Maintenance` and `Nebulex.TinyLFU.Maintenance.Supervisor`.

## Hot Path vs Cold Path

The architecture separates cache operations into two distinct paths:

- **Hot path** (client-facing): `get`, `put`, and `delete` operations read/write
  the ETS data table **directly** and append an event to a buffer. No GenServer
  calls, no message passing. Returns immediately.

- **Cold path** (maintenance): Buffer processors periodically drain events into
  the Maintenance Queue. The queue's single processor updates deques and runs
  eviction/admission. All deque mutations are serialized through this single
  writer — no locks or coordination needed.

This separation means cache operations never block on eviction decisions.

## Admission Flow

When a new entry is written to the cache:

```ascii
  ┌─────────┐     ┌────────────┐     ┌────────────────┐
  │  Client │────>│  Window    │────>│  Probation     │
  │  put(k) │     │  (add key) │     │  (if admitted) │
  └─────────┘     └─────┬──────┘     └────────────────┘
                        │
                  Window full?
                        │
                       yes
                        │
                        ▼
               ┌──────────────────┐
               │  Evict LRU from  │
               │  Window          │
               │  (window_victim) │
               └────────┬─────────┘
                        │
                        ▼
               ┌─────────────────────────────────────┐
               │  Peek LRU from Probation            │
               │  (main_victim)                      │
               │                                     │
               │  freq(window_victim)                │
               │    > freq(main_victim)?             │
               │                                     │
               │  YES: admit window_victim           │
               │       to Probation,                 │
               │       evict main_victim             │
               │                                     │
               │  NO:  discard window_victim         │
               └─────────────────────────────────────┘
```

The FrequencySketch provides the `freq()` estimates. This comparison is the core
of the TinyLFU admission policy — it only admits entries that are likely more
valuable than what's already in the cache.

## Promotion Flow

When a Probation entry is accessed (read):

```ascii
  ┌───────────────┐     ┌──────────────┐
  │  Probation    │────>│  Protected   │
  │  (remove key) │     │  (add key)   │
  └───────────────┘     └──────┬───────┘
                              │
                        Protected full?
                              │
                             yes
                              │
                              ▼
                     ┌─────────────────┐
                     │  Demote LRU     │
                     │  from Protected │
                     │  back to        │
                     │  Probation      │
                     └─────────────────┘
```

## Sequence: `cache.get(key)`

```ascii
  Client                ETS          Read Buffer       Maintenance Queue
    │                    │               │                      │
    │── lookup(key) ────>│               │                      │
    │<── value ──────────│               │                      │
    │                    │               │                      │
    │── put_newer(key, :read, ts) ──────>│                      │
    │                    │               │                      │
    │                    │   (periodic drain)                   │
    │                    │               │── push({key,:read})─>│
    │                    │               │                      │
    │                    │               │     (periodic drain) │
    │                    │               │                      │── touch key in
    │                    │               │                      │   its deque
    │                    │               │                      │   (or promote
    │                    │               │                      │   if in Probation)
```

1. Client reads from ETS directly — returns value immediately.
2. Client appends a read event to the Read Buffer via `put_newer` (deduped).
3. Read Buffer processor periodically pushes deduped events to the Maintenance
   Queue.
4. Maintenance Queue processor touches the key in whichever deque it's in. If
   the key is in Probation, it gets promoted to Protected.

## Sequence: `cache.put(key, value)`

```ascii
  Client                ETS          Write Buffer      Maintenance Queue
    │                    │               │                     │
    │── insert(key,val)─>│               │                     │
    │                    │               │                     │
    │── put_newer(key, {:write,val}, ts)>│                     │
    │                    │               │                     │
    │                    │   (periodic drain)                  │
    │                    │               │── push({key,        │
    │                    │               │   {:write,val}}) ──>│
    │                    │               │                     │
    │                    │               │    (periodic drain) │
    │                    │               │                     │── increment(key)
    │                    │               │                     │   in sketch
    │                    │               │                     │── add to Window
    │                    │               │                     │── if Window full:
    │                    │               │                     │   run admission
```

1. Client writes to ETS directly.
2. Client appends a write event to the Write Buffer via `put_newer` (deduped).
3. Write Buffer processor periodically pushes deduped events to the Maintenance
   Queue.
4. Maintenance Queue processor increments the key in the FrequencySketch, adds
   it to the Window deque, and runs the admission flow if the Window is full.

## Sequence: `cache.delete(key)`

```ascii
  Client                ETS          Write Buffer      Maintenance Queue
    │                    │               │                     │
    │── delete(key) ────>│               │                     │
    │                    │               │                     │
    │── put_newer(key, :delete, ts) ────>│                     │
    │                    │               │                     │
    │                    │   (periodic drain)                  │
    │                    │               │── push({key,        │
    │                    │               │   :delete}) ───────>│
    │                    │               │                     │
    │                    │               │    (periodic drain) │
    │                    │               │                     │── remove key from
    │                    │               │                     │   its deque
```

1. Client deletes from ETS directly.
2. Client appends a delete event to the Write Buffer.
3. The Maintenance Queue processor removes the key from whichever deque it's in.

## Supervision Tree

The adapter uses a two-supervisor design:

```ascii
Nebulex.TinyLFU.Supervisor (:rest_for_one)
  ├── AccessOrderDeque  (Window)
  ├── AccessOrderDeque  (Probation)
  ├── AccessOrderDeque  (Protected)
  └── Nebulex.TinyLFU.Maintenance.Supervisor (:rest_for_one)
        ├── Maintenance Queue  (Tidefall.Queue, 1 partition)
        ├── Read Buffer        (Tidefall.HashMap, N partitions)
        └── Write Buffer       (Tidefall.HashMap, N partitions)
```

**Why two supervisors?**

The outer supervisor starts the deques first. The inner supervisor starts after
the deques are up, fetches their structs via `AccessOrderDeque.get_deque/1`,
creates the FrequencySketch, and builds MFA-based processor callbacks that
reference these components.

Both use `:rest_for_one` strategy:

- **Outer**: If a deque crashes, the Maintenance Supervisor restarts too,
  re-fetching fresh deque references.
- **Inner**: If the Maintenance Queue crashes, the buffers restart too,
  ensuring they reference a valid queue.

## Design Decisions

### Why two-table ETS for LRU instead of a doubly-linked list?

Caffeine uses a doubly-linked list (DLL) with prev/next pointers on each node
for O(1) touch operations. In Java, HashMap lookup returns a mutable reference —
you can mutate prev/next pointers in-place.

In Elixir/ETS, lookup returns a **copy**, not a reference. Emulating a DLL would
require 8+ ETS operations per touch plus serialization to prevent pointer
corruption. The two-table approach gives O(log n) touch (ETS `ordered_set` is
a C-level AVL tree, `log2(1M) ~ 20` comparisons) with simpler code and fewer
failure modes.

### Why Tidefall instead of ring buffers?

Caffeine uses striped ring buffers for reads and an MPSC queue for writes. We
use `Tidefall.HashMap` with `put_newer` which provides:

- **Deduplication**: 1000 writes to the same key = 1 entry in the buffer.
- **Partitioned ETS**: One table per CPU for write scalability.
- **Double-buffering**: Zero-downtime processing — new events go to a fresh
  table while the old one is being drained.
- **Arbitrary-term keys**: the `:key_hasher` buffer option (default `true`,
  `:erlang.phash2`) lets any term be used as a cache key; a custom `fun/1`
  gives collision-free key identity. Early draining can be tuned with
  `:drain_threshold` / `:drain_check_interval`.

### Why a single-partition Maintenance Queue?

Eviction and admission decisions are cross-key and cross-deque — they cannot be
safely parallelized. A single partition ensures all deque mutations are
serialized through one processor (single writer), with no locks or coordination.

The write side is still concurrent: buffer processors insert into the queue's
ETS table concurrently (`:public` with `write_concurrency: true`).

### Why MFA tuples instead of closures for processors?

Tidefall processors are configured at startup and invoked repeatedly.
MFA tuples (`{Module, :function, [args]}`) are more explicit, easier to inspect
in crash logs, and avoid capturing large terms in closure environments.

## References

- [TinyLFU: A Highly Efficient Cache Admission Policy](https://dl.acm.org/citation.cfm?id=3149371)
- [An Improved Data Stream Summary: The Count-Min Sketch and its Applications](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)
- [Caffeine](https://github.com/ben-manes/caffeine) by Ben Manes
- [Tidefall](https://github.com/cabol/tidefall) — ETS-based partitioned buffer
  with double-buffering and coalescing (a maintained fork of PartitionedBuffer)
