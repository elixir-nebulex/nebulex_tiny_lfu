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
