defmodule Nebulex.Adapters.TinyLFU.Options do
  @moduledoc false

  # Start options
  start_opts = [
    stats: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      A flag to determine whether to collect cache stats.
      """
    ],
    max_size: [
      type: :pos_integer,
      required: false,
      doc: """
      The maximum number of entries to store in the cache. When set, this
      controls the total capacity across all segments and enables eviction.
      When omitted, the cache is unbounded (no eviction).

      Segment sizes are derived from this value following Caffeine's defaults:

        * Window: 1% of max_size
        * Main (probation + protected): 99% of max_size
        * Protected: 80% of main

      """
    ],
    read_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      ETS read concurrency option for the data table. See `:ets.new/2`.
      """
    ],
    write_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      ETS write concurrency option for the data table. See `:ets.new/2`.
      """
    ],
    buffer_opts: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Options for the internal read/write buffers and maintenance queue,
      passed through to `Tidefall`. The maintenance queue always overrides
      `:partitions` to 1 (single-writer guarantee).

      See Tidefall for the full semantics of each option —
      [start options](https://tidefall.hexdocs.pm/Tidefall.HashMap.html#module-start-options)
      (`:processing_interval`, `:processing_timeout`, `:processing_batch_size`,
      `:partitions`, `:drain_threshold`, `:drain_check_interval`) and
      [runtime options](https://tidefall.hexdocs.pm/Tidefall.HashMap.html#module-runtime-options)
      (`:key_hasher`). The per-option docs below only note where this adapter's
      default or behavior differs from Tidefall's.
      """,
      keys: [
        processing_interval: [
          type: :pos_integer,
          required: false,
          default: :timer.seconds(1),
          doc: """
          Buffer drain interval, in milliseconds. Defaults to 1 second here
          (Tidefall's own default is 5 seconds).
          """
        ],
        processing_timeout: [
          type: :timeout,
          required: false,
          default: :timer.seconds(30),
          doc: """
          Processing-task timeout, in milliseconds. Defaults to 30 seconds
          here (Tidefall's own default is 1 minute).
          """
        ],
        processing_batch_size: [
          type: :pos_integer,
          required: false,
          default: 100,
          doc: """
          Entries read from the buffer per batch. Defaults to 100 here
          (Tidefall's own default is 10).
          """
        ],
        partitions: [
          type: :pos_integer,
          required: false,
          doc: """
          Number of partitions for the read and write buffers. No adapter
          default — uses Tidefall's (`System.schedulers_online()`).
          """
        ],
        drain_threshold: [
          type: :pos_integer,
          required: false,
          doc: """
          Per-partition item count that triggers an early drain, in addition
          to the interval timer. When unset, this adapter derives a
          Caffeine-aligned default per buffer: write buffer
          `max(1, min(128, div(max_size, partitions)))`, read buffer `64`,
          maintenance queue `1`. An explicit value applies to all three
          buffers. The derived defaults are validated by the drain-tuning
          benchmark (`benchmarks/drain_tuning.exs`).
          """
        ],
        drain_check_interval: [
          type: :pos_integer,
          required: false,
          doc: """
          Poll interval (ms) for the early-drain size check. Defaults to
          `max(50, div(processing_interval, 10))` here — 100ms at the default
          `:processing_interval` (Tidefall's own default is 1 second). This
          bounds the worst-case policy lag at roughly twice this value; keep
          it below `:processing_interval`.
          """
        ],
        key_hasher: [
          type: {:or, [:boolean, {:fun, 1}]},
          required: false,
          default: true,
          doc: """
          How cache keys are hashed for the internal buffers. This adapter
          **defaults to `true`** (`:erlang.phash2`), unlike Tidefall's own
          `false` default, so any term works as a cache key out of the box —
          the versioned buffer path otherwise raises on map-containing keys.
          Pass `false` to disable hashing on simple-key-only caches, or a
          `fun/1` (e.g. `&:erlang.term_to_binary(&1, [:deterministic])`) for
          collision-free key identity.

          Cache-specific caveat: with `true`, a rare 28-bit `phash2` collision
          can — under very high key cardinality — leave an entry untracked by
          the eviction policy, so it is not counted toward `:max_size` until
          its TTL or an explicit delete (cached **values** are never affected).
          Use a collision-free `fun/1` if that matters for your workload.
          """
        ]
      ]
    ]
  ]

  # Nebulex common option keys
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++
                    Nebulex.Cache.Options.__start_opts__()

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts)

  ## Docs API

  # coveralls-ignore-start

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_schema)
  end

  # coveralls-ignore-stop

  ## Validation API

  @spec validate_start_opts!(keyword()) :: keyword()
  def validate_start_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.drop(@nbx_start_opts)
      |> NimbleOptions.validate!(@start_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end
end
