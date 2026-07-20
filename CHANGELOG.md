# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial release — a local Window TinyLFU (W-TinyLFU) cache adapter for Nebulex,
a port of [Caffeine](https://github.com/ben-manes/caffeine)'s admission policy.

### Added

- **W-TinyLFU admission policy** — a 4-bit Count-Min Sketch with periodic aging
  decides which entries are worth keeping, outperforming plain LRU on skewed
  access patterns and rejecting scan-style noise.
- **Lock-free hot path** — `get`, `put`, and `delete` hit ETS directly and
  append to a deduplicated buffer; no GenServer calls block the caller. Policy
  and eviction work runs asynchronously in a single-writer maintenance worker.
- **Segmented-LRU eviction** — three deques (Window → Probation → Protected)
  protect frequently-accessed entries while still admitting new arrivals.
- **Bounded or unbounded** — set `:max_size` for a managed cache with eviction
  (an eventual upper bound), or omit it for a plain ETS cache with no eviction
  overhead.
- **TTL with on-demand expiration** — expired entries are removed on next read;
  no per-entry timers.
- **Arbitrary-term keys** — the `:key_hasher` buffer option (default `true`)
  lets any term, including map-containing keys, be used as a cache key; pass a
  `fun/1` for collision-free key identity or `false` to disable hashing.
- **Configurable buffering** — `:buffer_opts` exposes the underlying
  [`:tidefall`](https://github.com/cabol/tidefall) buffer knobs:
  `:processing_interval`, `:processing_timeout`, `:processing_batch_size`,
  `:partitions`, `:drain_threshold`, `:drain_check_interval`, and
  `:key_hasher`.
- Standard `Nebulex.Cache` KV, Queryable, Info, Observable, and stats support.
- Built on `:ets` with `:atomics` for the frequency sketch and `:tidefall` for
  the read/write event buffers and maintenance queue.
