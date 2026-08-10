# Nebulex TinyLFU :coffee: (Still WIP :construction:)
>
> A high-throughput W-TinyLFU local cache adapter for [Nebulex][Nebulex].

[Nebulex]: https://github.com/cabol/nebulex

![CI](https://github.com/elixir-nebulex/nebulex_tiny_lfu/workflows/CI/badge.svg)
[![Codecov](https://codecov.io/gh/elixir-nebulex/nebulex_tiny_lfu/graph/badge.svg)](https://codecov.io/gh/elixir-nebulex/nebulex_tiny_lfu)
[![Hex Version](https://img.shields.io/hexpm/v/nebulex_tiny_lfu.svg)](https://hex.pm/packages/nebulex_tiny_lfu)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/nebulex_tiny_lfu)

## About

A Nebulex adapter implementing the **Window TinyLFU (W-TinyLFU)** cache
admission policy — a port of [Caffeine][caffeine], the reference Java
implementation. W-TinyLFU combines recency (LRU) and frequency (TinyLFU)
to deliver near-optimal hit rates across mixed workloads while keeping
the hot path lock-free.

[caffeine]: https://github.com/ben-manes/caffeine

Highlights:

* **W-TinyLFU admission** — a 4-bit Count-Min Sketch with periodic
    aging decides which entries are worth keeping. Outperforms plain LRU
    on most real workloads, especially scan-resistant access patterns.
* **Lock-free hot path** — `cache.get/1`, `cache.put/2`, and friends
    hit ETS directly and append to a deduplicated buffer. No GenServer
    calls, no policy work blocking the caller.
* **Segmented LRU** — three deques (Window → Probation → Protected)
    protect frequently-accessed entries while still admitting new
    arrivals.
* **Bounded or unbounded** — set `:max_size` for managed eviction, or
    omit it for a plain ETS cache with zero eviction overhead.
* **TTL with on-demand expiration**, queryable, observable, info, and
    stats support out of the box.

See the [module documentation][online_docs] for a deeper look at the
architecture, the admission/eviction flow, and tuning notes.

[online_docs]: https://hexdocs.pm/nebulex_tiny_lfu

## How it compares

Both numbers below come from the scripts in [benchmarks/](./benchmarks),
which reproduce them end to end (see [Benchmarks](#benchmarks)).

### Against Caffeine

Does the W-TinyLFU label hold? Replaying the standard cache-trace corpus
(ARC S3/DS1, LIRS gli/loop) through the adapter with a synchronized policy
(`benchmarks/trace_replay.exs`) lands within ~1 point of Caffeine's
published W-TinyLFU hit rates on 11 of the 12 published trace × size points,
worst delta −2.3. The loop trace has no published number; against an
analytic scan-resistance ideal it lands within 1.3 points at three of four
sizes, worst −4.6. A sample point per trace:

| trace (requests)  | cache size | this adapter | Caffeine W-TinyLFU | LRU   |
| ----------------- | ---------- | ------------ | ------------------ | ----- |
| ARC S3 (16.4M)    | 500,000    | 49.9%        | ~51%               | 22.8% |
| ARC DS1 (43.7M)   | 5,000,000  | 51.2%        | ~52%               | 21.0% |
| LIRS gli (6k)     | 1,000      | 49.7%        | ~50%               | 11.2% |
| LIRS loop (505k)  | 750        | 69.6%        | —                  | 0.0%  |

Caffeine references are read from the [Caffeine wiki's Efficiency
charts][caffeine-efficiency] (±1pt); the LRU column is an exact LRU
simulated on the same parsed traces. The script header documents the known
differences: Caffeine's admission jitter, window rounding, and its adaptive
window sizing against the fixed 1% window here, which shows up most on
recency-biased traces.

### Against `Nebulex.Adapters.Local`

Local's raw operations are ~6–8× faster. A `get` there is a bare ETS lookup
(~0.5 µs median), while this adapter's read path also writes a policy event
on every hit (~3.7 µs). What that buys is hit rate: at equal worst-case
capacity this adapter scores 2–10× higher on the same trace corpus
(S3 @ 500k: 49.9% vs 11.4%; DS1 @ 5M: 51.2% vs 18.3%; loop: 97.6% vs 0.0%,
since generational clearing collapses on looping and scanning patterns the
same way LRU does), and `max_size` enforcement under saturating writes stays
~3× tighter (`benchmarks/adapter_comparison.exs`).

Rule of thumb: if a miss costs more than a few microseconds (a query, an
RPC), the hit rate is what matters. If the cache itself is your hot path and
misses are cheap, Local's raw speed wins.

[caffeine-efficiency]: https://github.com/ben-manes/caffeine/wiki/Efficiency

## Installation

Add `:nebulex_tiny_lfu` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_tiny_lfu, "~> 3.0"},
    {:telemetry, "~> 0.4 or ~> 1.0"}, # For observability/telemetry support
    {:decorator, "~> 1.4"},           # For declarative caching
  ]
end
```

The `:telemetry` (observability and monitoring of cache operations) and
`:decorator` (declarative caching) dependencies are optional but highly
recommended for production use.

## Usage

Define your cache:

```elixir
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.TinyLFU
end
```

Configure it in `config/config.exs`:

```elixir
config :my_app, MyApp.Cache,
  max_size: 100_000
```

Add it to your application's supervision tree:

```elixir
def start(_type, _args) do
  children = [
    {MyApp.Cache, []},
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

Then use the standard `Nebulex.Cache` API:

```elixir
MyApp.Cache.put!("user:42", user)
MyApp.Cache.fetch!("user:42")
MyApp.Cache.delete!("user:42")
```

See the [online documentation][online_docs] for configuration options,
the `:max_size` eventual-consistency model, and the full architecture
walkthrough.

## Testing

Since this adapter uses support modules and shared tests from `Nebulex`,
but the test folder is not included in the Hex dependency, the following
steps are required to run the tests.

First, set the environment variable `NEBULEX_PATH` to `nebulex`:

```
export NEBULEX_PATH=nebulex
```

Second, fetch the `:nebulex` dependency directly from GitHub:

```
mix nbx.setup
```

Third, fetch deps:

```
mix deps.get
```

Finally, run the tests:

```
mix test
```

Running tests with coverage:

```
mix coveralls.html
```

You will find the coverage report within `cover/excoveralls.html`.

## Benchmarks

The adapter ships with benchmarks based on
[benchee](https://github.com/PragTob/benchee), located in
[benchmarks/](./benchmarks).

To run a benchmark:

```
MIX_ENV=test mix run benchmarks/BENCH_TEST_FILE
```

Where `BENCH_TEST_FILE` can be any of:

* `frequency_sketch.exs` — micro-benchmark for the Count-Min Sketch
    used by the admission filter.
* `drain_tuning.exs` — measures policy-side outcomes (hit rate, `max_size`
    overshoot, policy lag, maintenance churn) across buffer drain
    configurations, plus a hot-path guardrail. See the file header for the
    environment knobs.
* `trace_replay.exs` — replays the standard cache-trace corpus (ARC S3/DS1,
    LIRS gli/loop) with a synchronized policy and compares hit rates against
    Caffeine's published W-TinyLFU numbers plus an exact LRU baseline. The
    file header covers the methodology and where to get the traces, which
    aren't bundled because some have unclear licenses.
* `adapter_comparison.exs` — head-to-head against `Nebulex.Adapters.Local`:
    per-op latency (Benchee), saturating mixed throughput, and `max_size`
    enforcement under pressure. Pair it with `TRACE_ADAPTER=local` on
    `trace_replay.exs` for the capacity-comparable hit-rate half.

## Contributing

Contributions to Nebulex are very welcome and appreciated!

Use the [issue tracker](https://github.com/elixir-nebulex/nebulex_tiny_lfu/issues)
for bug reports or feature requests. Open a
[pull request](https://github.com/elixir-nebulex/nebulex_tiny_lfu/pulls)
when you are ready to contribute.

When submitting a pull request you should not update the
[CHANGELOG.md](CHANGELOG.md), and you should make sure your changes are
covered by tests — include unit tests alongside any new or changed code.

Before submitting a PR, run `mix test.ci` and make sure all checks pass.

## Copyright and License

Copyright (c) 2026 Carlos Andres Bolaños R.A.

`nebulex_tiny_lfu` source code is licensed under the
[MIT License](LICENSE.md).
