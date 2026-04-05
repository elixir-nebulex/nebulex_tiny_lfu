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
      Options for the internal read/write buffers and maintenance queue.
      These are passed through to `PartitionedBuffer`. The maintenance
      queue always overrides `:partitions` to 1 (single-writer guarantee).
      """,
      keys: [
        processing_interval_ms: [
          type: :pos_integer,
          required: false,
          default: :timer.seconds(1),
          doc: """
          How often (in milliseconds) each partition checks its buffer and
          initiates processing. Lower values mean faster processing but more
          frequent task spawning.
          """
        ],
        processing_timeout_ms: [
          type: :timeout,
          required: false,
          default: :timer.seconds(30),
          doc: """
          Maximum time (in milliseconds) for a processing task to complete
          before being forcefully terminated.
          """
        ],
        processing_batch_size: [
          type: :pos_integer,
          required: false,
          default: 100,
          doc: """
          Number of entries to read from the buffer per batch. The processor
          is called once per batch.
          """
        ],
        partitions: [
          type: :pos_integer,
          required: false,
          doc: """
          Number of buffer partitions for read and write buffers. More
          partitions reduce lock contention but increase per-partition
          overhead.
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
