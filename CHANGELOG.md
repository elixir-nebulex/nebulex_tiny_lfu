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
- **Caffeine-aligned drain defaults** — when `drain_*` keys are not set,
  per-buffer early-drain thresholds are derived from `:max_size` and the
  processing interval (write buffer `max(1, min(128, max_size/partitions))`,
  read buffer `64`, maintenance queue `1`, checked every
  `processing_interval/10` with a 50ms floor). This bounds worst-case policy
  lag at roughly twice the check interval instead of twice the processing
  interval; explicit `drain_*` values always win. Backed by the drain-tuning
  benchmark (`benchmarks/drain_tuning.exs`).
- **Validated hit rates** — `benchmarks/trace_replay.exs` replays the standard
  cache-trace corpus (ARC S3/DS1, LIRS gli/loop) with a synchronized policy and
  compares against Caffeine's published W-TinyLFU numbers: within ~1 point on
  11 of the 12 published trace × size points.
  `benchmarks/adapter_comparison.exs` measures the trade-off against
  `Nebulex.Adapters.Local`; see the README's "How it compares" section.
- Standard `Nebulex.Cache` KV, Queryable, Info, Observable, and stats support.
- Built on `:ets` with `:atomics` for the frequency sketch and `:tidefall` for
  the read/write event buffers and maintenance queue.
