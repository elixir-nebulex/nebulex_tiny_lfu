defmodule Nebulex.Adapters.TinyLFU do
  @moduledoc """
  Local cache adapter implementing the Window TinyLFU (W-TinyLFU) admission
  policy — a port of [Caffeine](https://github.com/ben-manes/caffeine), the
  reference Java implementation.

  W-TinyLFU combines recency (LRU) and frequency (TinyLFU admission filter)
  to deliver near-optimal hit rates across mixed workloads while keeping the
  hot path lock-free. Cached entries live in a single ETS table; the
  admission policy and segmented-LRU bookkeeping run asynchronously in a
  maintenance worker, so `cache.get/1`, `cache.put/2`, and friends never
  block on policy decisions.

  ## Features

    * **W-TinyLFU admission policy** — a 4-bit Count-Min Sketch with periodic
      aging decides which entries are worth keeping. Outperforms pure LRU on
      most real workloads, especially those with skewed access patterns.
    * **Lock-free hot path** — reads and writes hit ETS directly and append
      to a deduplicated buffer. No GenServer calls, no synchronous policy
      work blocking the caller.
    * **Segmented-LRU eviction** — three deques (Window → Probation →
      Protected) protect frequently-accessed entries while still admitting
      new arrivals.
    * **Bounded or unbounded** — set `:max_size` for a managed cache, or
      omit it for a plain ETS cache with no eviction overhead.
    * **TTL with on-demand expiration** — expired entries are removed when
      next read; no per-entry timers.
    * **Standard Nebulex queryable, info, observable, and stats support.**
    * **Pluggable runtime** — built on `:ets` with `:atomics` for the sketch
      and `PartitionedBuffer` for the read/write event buffers.

  ## How W-TinyLFU works

  ### Cache segments

  The bounded cache is split into three segments. Their sizes are derived
  from `:max_size` following Caffeine's defaults:

    * **Window** (≈ 1% of `max_size`) — every new entry lands here. Acts as
      a recency filter; an entry only graduates after it survives admission
      against the main cache.
    * **Probation** (≈ 20% of the remaining 99%) — holds entries admitted
      from the window. Candidates for long-term retention.
    * **Protected** (≈ 80% of the remaining 99%) — entries that were
      re-accessed while in probation. The "hot set" the cache aims to keep.

  ### Hot path vs. cold path

  ```ascii
    cache.get/put/delete    ┌────────────────┐         ┌────────────┐
    ────────────────────▶   │  ETS Data Tab  │ + event │ Read/Write │
    (direct, lock-free)     │   (key,val,…)  │ ──────▶ │   Buffers  │
                            └────────────────┘         │ (PB.Map,   │
                                                       │  deduped)  │
                                                       └─────┬──────┘
                                                             │ flush
                                                             ▼
                                            ┌──────────────────────────┐
                                            │   Maintenance Queue      │
                                            │   (PB.Queue, single      │
                                            │    writer processor)     │
                                            └────────────┬─────────────┘
                                                         │ drain batch
                          ┌──────────────────────────────┼──────────────────────────────┐
                          ▼                              ▼                              ▼
                    Window deque              Probation deque                 Protected deque
                    (AccessOrderDeque)        (AccessOrderDeque)              (AccessOrderDeque)
                          │
                          └────────────▶ FrequencySketch (admission decisions)
  ```

    * **Hot path** (`cache.get/put/delete`): direct ETS lookup/insert/delete,
      plus a `PartitionedBuffer.Map.put_newer` to enqueue the access event.
      Returns immediately. Reads and writes never serialise through a
      process.
    * **Cold path** (maintenance worker): on each flush cycle, buffered
      events are drained into a single-writer queue. The worker increments
      the frequency sketch, reorders the deques, runs admission, and evicts
      to honour `:max_size`. All deque mutations are serialised through one
      process, so no locks are needed inside the deques.

  Buffers deduplicate by key per flush window, so a hot key written 1000
  times in one window produces a single buffered event (with the original
  count preserved as `updates`, replayed against the sketch). This keeps
  maintenance throughput bounded by *unique keys per window*, not raw
  operation rate.

  ### Admission and eviction

  Every new entry enters the window deque at MRU. Once the window is full,
  its LRU is evicted and offered to the main cache:

    1. The candidate (window LRU) is compared against the **probation LRU**
       (the "victim") via the FrequencySketch.
    2. If `freq(candidate) > freq(victim)` → the candidate is admitted to
       probation and the victim is evicted from the cache.
    3. Otherwise the candidate is discarded (the more frequent victim wins).

  A read on a probation entry **promotes** it to protected; if protected is
  full, its LRU is **demoted** back to probation. After a write batch, if
  the total deque size still exceeds `max_size`, the worker continues
  evicting from the probation LRU (then protected, then window) until the
  cap is honoured.

  ## When to use

  ### Good fit

    * **High-throughput single-node caches** that need better hit rates
      than plain LRU.
    * **Mixed workloads** where some keys are bursty-hot and others are
      one-shot — the admission filter naturally rejects scan-style noise.
    * **Read-heavy applications** where even microseconds of hot-path
      contention add up.

  ### Not the right fit

    * **Distributed caches** — this adapter is single-node. For
      replication or sharding, use the `nebulex_distributed` adapters with
      this one (or `Nebulex.Adapters.Local`) as primary storage.
    * **Workloads that need a hard real-time `max_size` cap** — see
      ["max_size is an eventual upper bound"](#module-max-size-is-an-eventual-upper-bound).
    * **Tiny caches** — overhead of three deques, a sketch, and a
      maintenance worker isn't worth it for caches with a handful of
      entries. Plain `Nebulex.Adapters.Local` is simpler.

  ## Usage

      defmodule MyApp.Cache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.TinyLFU
      end

  Configure the cache in `config/config.exs`:

      config :my_app, MyApp.Cache,
        max_size: 100_000

  Add it to your application's supervision tree:

      def start(_type, _args) do
        children = [
          {MyApp.Cache, []},
          ...
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  Then use the standard `Nebulex.Cache` API:

      MyApp.Cache.put!("user:42", user)
      MyApp.Cache.fetch!("user:42")
      MyApp.Cache.delete!("user:42")

  See `Nebulex.Cache` for the full API.

  ### Unbounded mode

  If you omit `:max_size`, the cache becomes a plain ETS table with no
  eviction pipeline:

      config :my_app, MyApp.UnboundedCache,
        # max_size unset → unbounded
        read_concurrency: true,
        write_concurrency: true

  Unbounded mode skips the maintenance worker, deques, and frequency
  sketch entirely — useful for caches you size manually via TTL or
  explicit deletion.

  ## Configuration Options

  This adapter supports the following configuration options:

  #{Nebulex.Adapters.TinyLFU.Options.start_options_docs()}

  ## `max_size` is an eventual upper bound

  The `:max_size` option caps the number of entries the policy will retain,
  but enforcement happens in the maintenance pipeline rather than on the
  hot path. Concretely:

    * `cache.put/2` and friends write to the ETS data table immediately.
    * The maintenance worker drains the buffered events on its next cycle,
      updates the deques, and evicts entries whose total deque size exceeds
      `max_size`.

  During a write burst this means ETS can momentarily hold more than
  `max_size` entries — enforcement catches up on the next maintenance
  cycle. Caffeine has the same eventual-consistency property; if you need
  a hard real-time cap, this isn't the adapter for you.

  ## Caveats

    * **Single-node only.** This adapter doesn't replicate, shard, or
      coordinate across nodes. Wrap it in a `nebulex_distributed` adapter
      if you need topology.
    * **Eventual `max_size` enforcement.** See the section above.
    * **Single-writer maintenance.** All deque mutations serialise through
      one process. Maintenance throughput scales with unique keys per
      flush window, not raw op rate (the buffers deduplicate). For most
      workloads this is plenty; for extreme key-cardinality bursts, tune
      `:processing_interval_ms` and `:processing_batch_size` (see options
      above).
    * **No `:replace` in `put_all/2`.** `put_all` supports `:put` and
      `:put_new`. Use single-key `replace/3` for conditional updates.

  ## References

    * Einziger, Friedman & Manes, *TinyLFU: A Highly Efficient Cache
      Admission Policy* — [paper](https://dl.acm.org/citation.cfm?id=3149371).
    * Cormode & Muthukrishnan, *Count-Min Sketch* —
      [paper](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf).
    * Ben Manes, [Caffeine](https://github.com/ben-manes/caffeine) — the
      reference Java implementation this adapter ports.

  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Utils
  import Record

  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.TinyLFU.Options
  alias Nebulex.Time
  alias Nebulex.TinyLFU.Maintenance

  # Cache entry stored in the ETS data table
  defrecordp(:entry, key: nil, value: nil, exp: nil)

  ## Nebulex.Adapter callbacks

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      Returns the ETS data table reference for this cache instance.
      """
      def data_table do
        %{data_tab: data_tab} = get_dynamic_cache() |> Nebulex.Cache.Registry.lookup()

        data_tab
      end
    end
  end

  @impl true
  def init(opts) do
    # Common options
    {telemetry_prefix, opts} = Keyword.pop!(opts, :telemetry_prefix)
    {telemetry, opts} = Keyword.pop!(opts, :telemetry)
    {cache, opts} = Keyword.pop!(opts, :cache)

    # Validate options
    opts = Options.validate_start_opts!(opts)

    # Options
    name = opts[:name] || cache
    max_size = Keyword.get(opts, :max_size)

    # Init stats_counter
    stats_counter =
      if Keyword.fetch!(opts, :stats) == true do
        Stats.init(telemetry_prefix)
      end

    # Create the ETS data table
    data_tab =
      :ets.new(cache, [
        :set,
        :public,
        keypos: entry(:key) + 1,
        read_concurrency: Keyword.fetch!(opts, :read_concurrency),
        write_concurrency: Keyword.fetch!(opts, :write_concurrency)
      ])

    # Build adapter metadata
    adapter_meta = %{
      cache: cache,
      name: name,
      telemetry: telemetry,
      telemetry_prefix: telemetry_prefix,
      stats_counter: stats_counter,
      data_tab: data_tab,
      max_size: max_size,
      started_at: DateTime.utc_now()
    }

    # Build child spec for the TinyLFU supervision tree.
    # When unbounded (max_size nil), start an empty supervisor — the cache
    # is a plain ETS table with no eviction pipeline.
    child_spec =
      if max_size do
        Supervisor.child_spec(
          {Nebulex.TinyLFU.Supervisor,
           name: name,
           max_size: max_size,
           data_tab: data_tab,
           buffer_opts: Keyword.fetch!(opts, :buffer_opts)},
          id: {__MODULE__, cache}
        )
      else
        %{
          id: {__MODULE__, cache},
          start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]},
          type: :supervisor
        }
      end

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV callbacks

  @impl true
  def fetch(%{data_tab: data_tab} = adapter_meta, key, _opts) do
    case :ets.lookup(data_tab, key) do
      [entry(key: ^key, value: value, exp: exp)] ->
        if alive?(exp) do
          :ok = schedule_read(adapter_meta, key)

          {:ok, value}
        else
          true = :ets.delete(data_tab, key)
          :ok = schedule_delete(adapter_meta, key)

          wrap_error Nebulex.KeyError, key: key, reason: :expired
        end

      [] ->
        wrap_error Nebulex.KeyError, key: key
    end
  end

  @impl true
  def put(%{data_tab: data_tab} = adapter_meta, key, value, on_write, ttl, keep_ttl?, _opts) do
    on_write
    |> upsert(adapter_meta, data_tab, key, value, ttl, keep_ttl?)
    |> wrap_ok()
  end

  @impl true
  def put_all(%{data_tab: data_tab} = adapter_meta, entries, on_write, ttl, _opts) do
    on_write
    |> upsert_all(adapter_meta, data_tab, entries, ttl)
    |> wrap_ok()
  end

  @impl true
  def delete(%{data_tab: data_tab} = adapter_meta, key, _opts) do
    true = :ets.delete(data_tab, key)
    :ok = schedule_delete(adapter_meta, key)
  end

  @impl true
  def take(%{data_tab: data_tab} = adapter_meta, key, _opts) do
    case :ets.take(data_tab, key) do
      [entry(key: ^key, value: value, exp: exp)] ->
        :ok = schedule_delete(adapter_meta, key)

        if alive?(exp) do
          {:ok, value}
        else
          wrap_error Nebulex.KeyError, key: key, reason: :expired
        end

      [] ->
        wrap_error Nebulex.KeyError, key: key
    end
  end

  @impl true
  def update_counter(%{data_tab: data_tab} = adapter_meta, key, amount, default, ttl, _opts) do
    # On-demand expiration: delete expired entry so update_counter starts fresh
    :ok = maybe_delete_expired(data_tab, key)

    entry = entry(key: key, value: default, exp: exp(ttl))
    count = :ets.update_counter(data_tab, key, {3, amount}, entry)
    :ok = schedule_write(adapter_meta, key, amount)

    {:ok, count}
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    case fetch(adapter_meta, key, opts) do
      {:ok, _} -> {:ok, true}
      {:error, _} -> {:ok, false}
    end
  end

  @impl true
  def ttl(%{data_tab: data_tab} = adapter_meta, key, _opts) do
    case :ets.lookup(data_tab, key) do
      [entry(key: ^key, exp: :infinity)] ->
        {:ok, :infinity}

      [entry(key: ^key, exp: exp)] ->
        remaining = exp - Time.now()

        if remaining > 0 do
          {:ok, remaining}
        else
          true = :ets.delete(data_tab, key)
          :ok = schedule_delete(adapter_meta, key)

          wrap_error Nebulex.KeyError, key: key, reason: :expired
        end

      [] ->
        wrap_error Nebulex.KeyError, key: key
    end
  end

  @impl true
  def expire(%{data_tab: data_tab}, key, ttl, _opts) do
    updated? = :ets.update_element(data_tab, key, {entry(:exp) + 1, exp(ttl)})

    {:ok, updated?}
  end

  @impl true
  def touch(%{data_tab: data_tab} = adapter_meta, key, _opts) do
    case :ets.lookup(data_tab, key) do
      [entry(key: ^key, exp: exp)] ->
        if alive?(exp) do
          :ok = schedule_read(adapter_meta, key)

          {:ok, true}
        else
          true = :ets.delete(data_tab, key)
          :ok = schedule_delete(adapter_meta, key)

          {:ok, false}
        end

      [] ->
        {:ok, false}
    end
  end

  ## Nebulex.Adapter.Queryable callbacks

  @impl true
  def execute(adapter_meta, query_spec, opts)

  def execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  def execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
      when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  def execute(%{data_tab: data_tab}, %{op: :get_all, query: {:in, keys}, select: select}, _opts) do
    data_tab
    |> select_get_all(keys, select)
    |> wrap_ok()
  end

  def execute(%{data_tab: data_tab}, %{op: :count_all, query: {:in, keys}}, _opts) do
    data_tab
    |> select_count_all(keys)
    |> wrap_ok()
  end

  def execute(%{data_tab: data_tab} = adapter_meta, %{op: :delete_all, query: {:in, keys}}, _opts) do
    data_tab
    |> select_delete_all(keys)
    |> tap(fn _ -> schedule_delete_all(adapter_meta, keys) end)
    |> wrap_ok()
  end

  def execute(%{data_tab: data_tab} = adapter_meta, %{op: :delete_all, query: {:q, nil}}, _opts) do
    count = :ets.info(data_tab, :size)
    true = :ets.delete_all_objects(data_tab)

    :ok = schedule_flush(adapter_meta)

    {:ok, count}
  end

  def execute(%{data_tab: data_tab}, %{op: op, query: {:q, ms}, select: select}, _opts) do
    ms
    |> assert_match_spec(select)
    |> maybe_match_spec_return_true(op)
    |> do_execute(data_tab, op)
    |> wrap_ok()
  end

  defp do_execute(ms, data_tab, :get_all) do
    :ets.select(data_tab, ms)
  end

  defp do_execute(ms, data_tab, :count_all) do
    :ets.select_count(data_tab, ms)
  end

  defp do_execute(ms, data_tab, :delete_all) do
    :ets.select_delete(data_tab, ms)
  end

  @impl true
  def stream(adapter_meta, query_spec, opts)

  def stream(%{data_tab: data_tab}, %{query: {:in, keys}, select: select}, opts) do
    keys
    |> Stream.chunk_every(Keyword.get(opts, :max_entries, 20))
    |> Stream.map(&:ets.select(data_tab, in_match_spec(&1, select)))
    |> Stream.flat_map(& &1)
    |> wrap_ok()
  end

  def stream(%{data_tab: data_tab}, %{query: {:q, nil}, select: select}, _opts) do
    Stream.resource(
      fn -> :ets.first(data_tab) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        key -> stream_next(data_tab, key, select)
      end,
      fn _ -> :ok end
    )
    |> wrap_ok()
  end

  def stream(%{data_tab: data_tab}, %{query: {:q, ms}, select: select}, opts) do
    ms = assert_match_spec(ms, select)
    page_size = Keyword.get(opts, :max_entries, 20)

    Stream.resource(
      fn -> :ets.select(data_tab, ms, page_size) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        {elements, cont} -> {elements, :ets.select(cont)}
      end,
      fn _ -> :ok end
    )
    |> wrap_ok()
  end

  ## Internal helpers

  defp upsert(:put, adapter_meta, data_tab, key, value, ttl, false = _keep_ttl?) do
    true = :ets.insert(data_tab, entry(key: key, value: value, exp: exp(ttl)))
    :ok = schedule_write(adapter_meta, key, value)

    true
  end

  defp upsert(:put, adapter_meta, data_tab, key, value, ttl, true = _keep_ttl?) do
    # Try to update only the value; if key doesn't exist, insert with TTL
    with false <- :ets.update_element(data_tab, key, {entry(:value) + 1, value}) do
      true = :ets.insert(data_tab, entry(key: key, value: value, exp: exp(ttl)))
    end

    :ok = schedule_write(adapter_meta, key, value)

    true
  end

  defp upsert(:put_new, adapter_meta, data_tab, key, value, ttl, _keep_ttl?) do
    with true <- :ets.insert_new(data_tab, entry(key: key, value: value, exp: exp(ttl))) do
      :ok = schedule_write(adapter_meta, key, value)

      true
    end
  end

  defp upsert(:replace, adapter_meta, data_tab, key, value, _ttl, true = _keep_ttl?) do
    with true <- :ets.update_element(data_tab, key, {entry(:value) + 1, value}) do
      :ok = schedule_write(adapter_meta, key, value)

      true
    end
  end

  defp upsert(:replace, adapter_meta, data_tab, key, value, ttl, false = _keep_ttl?) do
    updates = [{entry(:value) + 1, value}, {entry(:exp) + 1, exp(ttl)}]

    with true <- :ets.update_element(data_tab, key, updates) do
      :ok = schedule_write(adapter_meta, key, value)

      true
    end
  end

  defp upsert_all(on_write, adapter_meta, data_tab, entries, ttl) do
    exp = exp(ttl)
    records = Enum.map(entries, fn {key, value} -> entry(key: key, value: value, exp: exp) end)

    do_upsert_all(on_write, adapter_meta, data_tab, entries, records)
  end

  defp do_upsert_all(:put, adapter_meta, data_tab, entries, records) do
    true = :ets.insert(data_tab, records)
    :ok = schedule_write_all(adapter_meta, entries)

    true
  end

  defp do_upsert_all(:put_new, adapter_meta, data_tab, entries, records) do
    with true <- :ets.insert_new(data_tab, records) do
      :ok = schedule_write_all(adapter_meta, entries)

      true
    end
  end

  defp maybe_delete_expired(data_tab, key) do
    with [entry(key: ^key, exp: exp)] when is_integer(exp) <- :ets.lookup(data_tab, key) do
      if not alive?(exp), do: :ets.delete(data_tab, key)
    end

    :ok
  end

  defp exp(:infinity), do: :infinity
  defp exp(ttl) when is_integer(ttl), do: Time.now() + ttl

  defp alive?(:infinity), do: true
  defp alive?(exp), do: Time.now() < exp

  defp select_entry(key, value, {:key, :value}), do: {key, value}
  defp select_entry(key, _value, :key), do: key
  defp select_entry(_key, value, :value), do: value
  defp select_entry(key, value, :entry), do: {key, value}

  defp stream_next(data_tab, key, select) do
    case :ets.lookup(data_tab, key) do
      [entry(key: ^key, value: value, exp: exp)] ->
        next = :ets.next(data_tab, key)

        if alive?(exp),
          do: {[select_entry(key, value, select)], next},
          else: {[], next}

      # Race guard: a concurrent process deleted `key` from the data table
      # between the previous `:ets.first/next` (which produced this `key`)
      # and our `:ets.lookup`. We just skip this slot and continue. Hard
      # to trigger deterministically from a test, hence the ignore.
      # coveralls-ignore-next-line
      [] ->
        {[], :ets.next(data_tab, key)}
    end
  end

  ## ETS select helpers

  # Default chunk size for ETS select operations
  @select_chunk_size 10

  defp select_get_all(data_tab, keys, select) do
    return = select_return(select)

    ets_select_keys(
      keys,
      @select_chunk_size,
      [],
      &new_match_spec(&1, return),
      &:ets.select(data_tab, &1),
      &(&1 ++ &2),
      &match_unexpired_key/2
    )
  end

  defp select_count_all(data_tab, keys) do
    ets_select_keys(
      keys,
      @select_chunk_size,
      0,
      &new_match_spec/1,
      &:ets.select_count(data_tab, &1),
      &(&1 + &2),
      &match_unexpired_key/2
    )
  end

  defp select_delete_all(data_tab, keys) do
    ets_select_keys(
      keys,
      @select_chunk_size,
      0,
      &new_match_spec/1,
      &:ets.select_delete(data_tab, &1),
      &(&1 + &2),
      &match_key/2
    )
  end

  # Entry match variables: $1 = key, $2 = value, $3 = exp
  defp new_match_spec(conds, return \\ true) do
    [{entry(key: :"$1", value: :"$2", exp: :"$3"), [conds], [return]}]
  end

  # Match spec return value based on select option
  defp select_return({:key, :value}), do: {{:"$1", :"$2"}}
  defp select_return(:key), do: :"$1"
  defp select_return(:value), do: :"$2"
  defp select_return(:entry), do: {{:"$1", :"$2"}}

  # Match only key (for delete_all — deletes regardless of expiration)
  defp match_key(k, _now) do
    {:"=:=", :"$1", k}
  end

  # Match unexpired key (for get_all and count_all)
  defp match_unexpired_key(k, now) do
    {:andalso, {:"=:=", :"$1", k}, {:orelse, {:"=:=", :"$3", :infinity}, {:<, now, :"$3"}}}
  end

  # Validates a match spec query. `nil` builds a "match all unexpired" spec.
  # Any other value is validated with `:ets.test_ms/2`.
  #
  # The `Time.now/0` is captured once at spec build time, not re-evaluated
  # per row by ETS. This is intentional snapshot semantics: a streamed
  # query yields entries that were unexpired at the start of the call,
  # even if some pass their TTL mid-stream. Avoids a per-row clock read,
  # and matches the cache's general "expired entries surface lazily on
  # access" behaviour elsewhere in the adapter.
  defp assert_match_spec(nil, select) do
    [
      {
        entry(key: :"$1", value: :"$2", exp: :"$3"),
        [{:orelse, {:"=:=", :"$3", :infinity}, {:<, Time.now(), :"$3"}}],
        [select_return(select)]
      }
    ]
  end

  defp assert_match_spec(spec, _select) do
    case :ets.test_ms(test_ms(), spec) do
      {:ok, _result} ->
        spec

      {:error, _result} ->
        msg = """
        invalid query, expected one of:

        - `nil` - match all entries
        - `:ets.match_spec()` - ETS match spec

        but got:

        #{inspect(spec, pretty: true)}
        """

        raise Nebulex.QueryError, message: msg, query: spec
    end
  end

  defp test_ms, do: entry(key: 1, value: 1, exp: 1000)

  defp maybe_match_spec_return_true([{pattern, conds, _ret}], op)
       when op in [:delete_all, :count_all] do
    [{pattern, conds, [true]}]
  end

  defp maybe_match_spec_return_true(match_spec, _op) do
    match_spec
  end

  # Builds a match spec for a list of keys (used by stream {:in, keys})
  defp in_match_spec([k], select) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    match_unexpired_key(k, Time.now())
    |> new_match_spec(select_return(select))
  end

  defp in_match_spec([k1, k2 | keys], select) do
    k1 = if is_tuple(k1), do: tuple_to_match_spec(k1), else: k1
    k2 = if is_tuple(k2), do: tuple_to_match_spec(k2), else: k2
    now = Time.now()

    keys
    |> Enum.reduce(
      {:orelse, match_unexpired_key(k1, now), match_unexpired_key(k2, now)},
      fn k, acc ->
        k = if is_tuple(k), do: tuple_to_match_spec(k), else: k
        {:orelse, acc, match_unexpired_key(k, now)}
      end
    )
    |> new_match_spec(select_return(select))
  end

  # Entry point: single key
  defp ets_select_keys([k], chunk_size, acc, ms_fun, chunk_fun, after_fun, match_key) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      [],
      2,
      chunk_size,
      match_key.(k, Time.now()),
      acc,
      ms_fun,
      chunk_fun,
      after_fun,
      match_key
    )
  end

  # Entry point: two or more keys
  defp ets_select_keys([k1, k2 | keys], chunk_size, acc, ms_fun, chunk_fun, after_fun, match_key) do
    k1 = if is_tuple(k1), do: tuple_to_match_spec(k1), else: k1
    k2 = if is_tuple(k2), do: tuple_to_match_spec(k2), else: k2
    now = Time.now()

    ets_select_keys(
      keys,
      2,
      chunk_size,
      {:orelse, match_key.(k1, now), match_key.(k2, now)},
      acc,
      ms_fun,
      chunk_fun,
      after_fun,
      match_key
    )
  end

  # All keys processed — flush final chunk
  defp ets_select_keys(
         [],
         _count,
         _chunk_size,
         chunk_acc,
         acc,
         ms_fun,
         chunk_fun,
         after_fun,
         _match_key
       ) do
    chunk_acc
    |> ms_fun.()
    |> chunk_fun.()
    |> after_fun.(acc)
  end

  # Chunk is full — flush and continue
  defp ets_select_keys(
         keys,
         count,
         chunk_size,
         chunk_acc,
         acc,
         ms_fun,
         chunk_fun,
         after_fun,
         match_key
       )
       when count >= chunk_size do
    chunk_acc
    |> ms_fun.()
    |> chunk_fun.()
    |> after_fun.(acc)
    |> then(&ets_select_keys(keys, chunk_size, &1, ms_fun, chunk_fun, after_fun, match_key))
  end

  # Add key to current chunk
  defp ets_select_keys(
         [k | keys],
         count,
         chunk_size,
         chunk_acc,
         acc,
         ms_fun,
         chunk_fun,
         after_fun,
         match_key
       ) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      keys,
      count + 1,
      chunk_size,
      {:orelse, chunk_acc, match_key.(k, Time.now())},
      acc,
      ms_fun,
      chunk_fun,
      after_fun,
      match_key
    )
  end

  defp tuple_to_match_spec(data) do
    data
    |> :erlang.tuple_to_list()
    |> tuple_to_match_spec([])
  end

  defp tuple_to_match_spec([], acc) do
    {acc |> Enum.reverse() |> :erlang.list_to_tuple()}
  end

  defp tuple_to_match_spec([e | tail], acc) do
    e = if is_tuple(e), do: tuple_to_match_spec(e), else: e

    tuple_to_match_spec(tail, [e | acc])
  end

  ## Buffer event scheduling

  # Skip eviction when the cache is unbounded
  defp schedule_read(%{max_size: nil}, _key) do
    :ok
  end

  defp schedule_read(%{name: name}, key) do
    name
    |> Maintenance.read_buffer_name()
    |> PartitionedBuffer.Map.put_newer(key, :read, System.monotonic_time())
  end

  # Skip eviction when the cache is unbounded
  defp schedule_write(%{max_size: nil}, _key, _value) do
    :ok
  end

  defp schedule_write(%{name: name}, key, value) do
    name
    |> Maintenance.write_buffer_name()
    |> PartitionedBuffer.Map.put_newer(key, {:write, value}, System.monotonic_time())
  end

  # Skip eviction when the cache is unbounded
  defp schedule_write_all(%{max_size: nil}, _entries) do
    :ok
  end

  defp schedule_write_all(%{name: name}, entries) do
    version = System.monotonic_time()
    entries = Enum.map(entries, fn {key, value} -> {key, {:write, value}, version} end)

    name
    |> Maintenance.write_buffer_name()
    |> PartitionedBuffer.Map.put_all_newer(entries)
  end

  # Skip eviction when the cache is unbounded
  defp schedule_delete(%{max_size: nil}, _key) do
    :ok
  end

  defp schedule_delete(%{name: name}, key) do
    name
    |> Maintenance.write_buffer_name()
    |> PartitionedBuffer.Map.put_newer(key, :delete, System.monotonic_time())
  end

  # Skip eviction when the cache is unbounded
  defp schedule_delete_all(%{max_size: nil}, _keys) do
    :ok
  end

  defp schedule_delete_all(%{name: name}, keys) do
    version = System.monotonic_time()
    entries = Enum.map(keys, fn key -> {key, :delete, version} end)

    name
    |> Maintenance.write_buffer_name()
    |> PartitionedBuffer.Map.put_all_newer(entries)
  end

  # Skip eviction when the cache is unbounded
  defp schedule_flush(%{max_size: nil}) do
    :ok
  end

  defp schedule_flush(%{name: name}) do
    name
    |> Maintenance.write_buffer_name()
    |> PartitionedBuffer.Map.put_newer(
      Maintenance.flush_key(),
      :flush_all,
      System.monotonic_time()
    )
  end
end
