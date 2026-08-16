# Trace-replay hit-rate validation against Caffeine's published numbers.
#
# Replays the standard cache-trace corpus (ARC S3/DS1, LIRS gli/loop) through
# the adapter with a synchronous drain after every N accesses, so the policy
# sees each access before the next one. That's the same contract as
# Caffeine's simulator, which this validation compares against. See
# `docs/plans/2026-08-09-002-feat-trace-replay-and-comparative-bench-plan.md`
# for the design and `docs/bench/TRACE_RESULTS.md` (local-only) for results.
#
# Trace files are expected under `docs/bench/traces/` (gitignored). Do NOT
# commit them; some have unclear licenses.
#
#   * `S3.lis`, `DS1.lis` — ARC traces (Megiddo & Modha, IBM), mirrored at
#     https://github.com/moka-rs/cache-trace (zstd-compressed)
#   * `gli.trace`, `loop.trace` — LIRS traces, bundled with Caffeine's
#     simulator under `simulator/src/main/resources/.../parser/lirs/`
#
# Run with:
#
#     MIX_ENV=test mix run benchmarks/trace_replay.exs [trace ...]
#
# where `trace` is any of `gli`, `loop`, `s3`, `ds1` (default: all four).
#
# Environment knobs (all optional):
#
#     TRACE_DIR=docs/bench/traces  # trace file location
#     TRACE_FLUSH_EVERY=1          # accesses between synchronous drains
#     TRACE_SIZES=250,500          # override cache sizes for every trace
#     TRACE_SKIP_LRU=1             # skip the exact LRU baseline simulation
#     TRACE_ADAPTER=tinylfu        # adapter under test: tinylfu | local
#
# `TRACE_ADAPTER=local` replays through `Nebulex.Adapters.Local` instead, for
# the policy-quality half of the adapter comparison (see
# `benchmarks/adapter_comparison.exs` for the throughput half). Local's
# periodic size check cannot bound the cache deterministically, so the replay
# rotates generations manually: each generation is capped at `size / 2`, and
# a new generation starts the moment the newer one exceeds it. Worst-case
# resident entries then equal the TinyLFU run's `max_size`, which makes the
# hit rates capacity-comparable. The `caffeine_wtlfu` and `delta` columns
# still refer to Caffeine's W-TinyLFU reference, so they read as "how far
# Local's generational eviction sits below the W-TinyLFU policy".
#
# ## Methodology (and deltas from Caffeine's simulator)
#
# Caffeine's simulator drives the policy synchronously: `record(key)` inserts
# on miss, so the first access to a key is a compulsory miss and hit rate is
# `hits / (hits + misses)`. This replay produces the same accounting through
# the adapter's public API: `fetch` then `put` on miss (cache-aside), with
# hit rate taken from the Nebulex stats delta.
#
# The adapter's policy pipeline is asynchronous (read/write buffers -> queue
# -> maintenance), so a full-speed replay would measure policy *lag* rather
# than policy *quality*. Tidefall exposes no synchronous flush primitive, so
# the replay bypasses the timers instead. Buffers are started with the drain
# timers pushed out of reach, and after every `TRACE_FLUSH_EVERY` accesses
# the replay drains the partition tables itself, in order: read buffer,
# write buffer, maintenance queue. It does that through the adapter's own
# processor callbacks, with the same maintenance context the cache built at
# startup, read out of the queue partition's state. Replaying in a single
# process keeps the select-then-delete on the partition tables race-free.
#
# Known differences from Caffeine's simulator, none of them in the adapter's
# favor:
#
#   * Caffeine's TinyLFU admission adds jitter (it admits a losing candidate
#     with ~0.78% probability once its frequency is >= 6) to break
#     hash-flooding ties. This adapter admits strictly by frequency.
#   * Caffeine sizes the window as `max - floor(0.99 * max)` (rounds up);
#     this adapter uses `max(1, div(max, 100))` (rounds down).
#   * With `TRACE_FLUSH_EVERY > 1`, same-key events within a batch coalesce
#     in the buffers (the sketch still sees every access via the coalesced
#     `updates` counter, but the deques see one touch per batch). Batch
#     draining also reorders events: all of the batch's reads are processed
#     before all of its writes, and order within each buffer is undefined
#     (the buffers are hash tables). Publish numbers from the default
#     `TRACE_FLUSH_EVERY=1` only, where none of this applies.

Code.require_file("support/helpers.exs", __DIR__)

defmodule Replay.Cache do
  @moduledoc false
  use Nebulex.Cache,
    otp_app: :nebulex_tiny_lfu,
    adapter: Nebulex.Adapters.TinyLFU
end

defmodule Replay.LocalCache do
  @moduledoc false
  use Nebulex.Cache,
    otp_app: :nebulex_tiny_lfu,
    adapter: Nebulex.Adapters.Local
end

defmodule Replay.Trace do
  @moduledoc false
  # Trace catalog and parsers. Parsing semantics mirror Caffeine's simulator
  # readers (`ArcTraceReader`, `LirsTraceReader`).

  # Reference hit rates (%) read from the charts on caffeine.wiki's
  # Efficiency page (`caffeine/wiki/efficiency/*.png`), accurate to ~±1pt.
  # `loop` has no published chart; its reference is analytic — a perfectly
  # scan-resistant policy pins `size` of the 1,011 loop keys, so the steady
  # state hit rate approaches `size / 1011` (compulsory misses amortize away
  # over 500 passes).
  @catalog %{
    "s3" => %{
      file: "S3.lis",
      format: :arc,
      label: "ARC S3 (search)",
      sizes: [100_000, 300_000, 500_000, 800_000],
      wtlfu_ref: [13.0, 33.0, 51.0, 70.0],
      lru_ref: [2.0, 8.0, 22.0, 56.0],
      ref_source: "caffeine.wiki Efficiency \"search\" chart (±1pt)"
    },
    "ds1" => %{
      file: "DS1.lis",
      format: :arc,
      label: "ARC DS1 (database)",
      sizes: [1_000_000, 3_000_000, 5_000_000, 8_000_000],
      wtlfu_ref: [15.0, 41.0, 52.0, 70.0],
      lru_ref: [3.0, 19.0, 21.0, 43.0],
      ref_source: "caffeine.wiki Efficiency \"database\" chart (±1pt)"
    },
    "gli" => %{
      file: "gli.trace",
      format: :lirs,
      label: "LIRS gli (glimpse)",
      sizes: [250, 500, 1_000, 2_000],
      wtlfu_ref: [16.0, 34.0, 50.0, 58.0],
      lru_ref: [1.0, 1.0, 13.0, 57.0],
      ref_source: "caffeine.wiki Efficiency \"glimpse\" chart (±1pt)"
    },
    "loop" => %{
      file: "loop.trace",
      format: :lirs,
      label: "LIRS loop (scan resistance)",
      sizes: [250, 500, 750, 1_000],
      wtlfu_ref: [24.7, 49.5, 74.2, 98.9],
      lru_ref: [0.0, 0.0, 0.0, 0.0],
      ref_source: "analytic (size / 1011 loop keys; LRU thrashes to ~0%)"
    }
  }

  def catalog, do: @catalog

  def fetch!(id) do
    case @catalog do
      %{^id => trace} -> Map.put(trace, :id, id)
      _other -> raise ArgumentError, "unknown trace #{inspect(id)}"
    end
  end

  @doc """
  Raises with acquisition instructions when the trace file is missing, so a
  cold run fails before any cache is started instead of mid-replay.
  """
  def check_file!(%{file: file}, dir) do
    path = Path.join(dir, file)

    unless File.exists?(path) do
      raise """
      trace file not found: #{path}

      Trace files are not committed (unclear licenses). See the header of
      benchmarks/trace_replay.exs for the download sources (ARC: moka-rs/cache-trace;
      LIRS: bundled with Caffeine's simulator), or set TRACE_DIR.
      """
    end

    :ok
  end

  @doc "Streams the trace's keys in access order."
  def stream(%{format: :arc, file: file}, dir) do
    # ARC format: `<start-block> <count> ...` per line — expands to `count`
    # sequential block keys. Remaining fields are ignored (request
    # type/number in the original `.lis` files).
    dir
    |> Path.join(file)
    |> File.stream!()
    |> Stream.reject(&(String.trim(&1) == ""))
    |> Stream.flat_map(fn line ->
      [start, count | _ignored] = String.split(line, " ", parts: 3)
      start = String.to_integer(start)

      start..(start + String.to_integer(String.trim(count)) - 1)//1
    end)
  end

  def stream(%{format: :lirs, file: file}, dir) do
    # LIRS format: one integer key per line; `*` checkpoint markers and
    # blank lines are skipped.
    dir
    |> Path.join(file)
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 in ["*", ""]))
    |> Stream.map(&String.to_integer/1)
  end
end

defmodule Replay.Drain do
  @moduledoc false
  # Synchronous drain of the maintenance pipeline. Resolves the buffer
  # partitions once (single-partition buffers — see the replay buffer_opts),
  # then `flush/1` empties read buffer -> write buffer -> maintenance queue
  # by invoking the adapter's processor callbacks directly, with the same
  # maintenance context the queue's own processor holds.

  alias Nebulex.TinyLFU.Maintenance

  def new(name) do
    queue_name = Maintenance.queue_name(name)

    %{
      read: partition!(Maintenance.read_buffer_name(name)),
      write: partition!(Maintenance.write_buffer_name(name)),
      queue: partition!(queue_name),
      queue_name: queue_name,
      ctx: maintenance_ctx!(queue_name)
    }
  end

  def flush(drain) do
    flush_buffer(drain.read, drain.queue_name)
    flush_buffer(drain.write, drain.queue_name)
    drain_queue(drain.queue, drain.ctx)
  end

  defp partition!(buffer_name) do
    case Registry.lookup(Tidefall.Registry, buffer_name) do
      [{_pid, partition}] ->
        partition

      other ->
        raise "expected a single partition for #{inspect(buffer_name)}, got: #{inspect(other)}"
    end
  end

  # The queue partition's processor MFA carries the maintenance context the
  # supervisor built at startup (sketch, deques, capacities, data table) —
  # reuse it so the synchronous drain drives the exact same policy state.
  defp maintenance_ctx!(queue_name) do
    [{pid, _partition}] = Registry.lookup(Tidefall.Registry, queue_name)

    case :sys.get_state(pid) do
      %{processor: {Maintenance, :process_maintenance, [ctx]}} ->
        ctx

      other ->
        raise "unexpected queue partition state: #{inspect(other)}"
    end
  end

  defp flush_buffer(partition, queue_name) do
    tab = Tidefall.Buffer.Partition.current_table(partition)

    case :ets.select(tab, Tidefall.HashMap.ets_match_spec()) do
      [] ->
        :ok

      entries ->
        true = :ets.delete_all_objects(tab)

        Maintenance.process_buffer(entries, queue_name)
    end
  end

  defp drain_queue(partition, ctx) do
    tab = Tidefall.Buffer.Partition.current_table(partition)

    case :ets.select(tab, Tidefall.Queue.ets_match_spec()) do
      [] ->
        :ok

      events ->
        true = :ets.delete_all_objects(tab)

        Maintenance.process_maintenance(events, ctx)
    end
  end
end

defmodule Replay.LRU do
  @moduledoc false
  # Exact LRU baseline simulated on the same parsed trace: an ordered_set of
  # `{seq, key}` provides the recency order, a set of `{key, seq}` provides
  # membership. Same accounting as the cache replay (first access is a
  # compulsory miss).

  def new(max_size) do
    %{
      order: :ets.new(:lru_order, [:ordered_set]),
      index: :ets.new(:lru_index, [:set]),
      max_size: max_size,
      seq: 0,
      hits: 0,
      misses: 0,
      size: 0
    }
  end

  def access(state, key) do
    case :ets.lookup(state.index, key) do
      [{^key, seq}] ->
        true = :ets.delete(state.order, seq)

        touch(%{state | hits: state.hits + 1}, key)

      [] ->
        state
        |> Map.update!(:misses, &(&1 + 1))
        |> Map.update!(:size, &(&1 + 1))
        |> touch(key)
        |> evict()
    end
  end

  def hit_rate(%{hits: hits, misses: misses}) when hits + misses > 0 do
    hits * 100 / (hits + misses)
  end

  def hit_rate(_state) do
    0.0
  end

  def delete(state) do
    :ets.delete(state.order)
    :ets.delete(state.index)

    :ok
  end

  defp touch(state, key) do
    seq = state.seq + 1

    true = :ets.insert(state.order, {seq, key})
    true = :ets.insert(state.index, {key, seq})

    %{state | seq: seq}
  end

  defp evict(%{size: size, max_size: max_size} = state) when size <= max_size do
    state
  end

  defp evict(state) do
    lru_seq = :ets.first(state.order)
    [{^lru_seq, key}] = :ets.lookup(state.order, lru_seq)

    true = :ets.delete(state.order, lru_seq)
    true = :ets.delete(state.index, key)

    %{state | size: state.size - 1}
  end
end

defmodule Replay.Runner do
  @moduledoc false

  alias Nebulex.Adapters.Local.Generation
  alias Replay.{Cache, Drain, LocalCache, LRU, Trace}

  # Sentinel interval that keeps Tidefall's own drain timers from ever
  # firing during a replay; the replay drains synchronously instead. ~41 days
  # (safely under Process.send_after's ~49.7-day cap) so even a pathologically
  # slow multi-hour size point cannot race the synchronous drain.
  @never :timer.hours(1_000)

  # Drain threshold high enough that the size-check timer (which itself only
  # fires every @never ms) can never trip it.
  @threshold_off 1_000_000_000

  def params! do
    adapter =
      case System.get_env("TRACE_ADAPTER", "tinylfu") do
        "tinylfu" -> :tinylfu
        "local" -> :local
        other -> raise ArgumentError, "unknown TRACE_ADAPTER #{inspect(other)}"
      end

    %{
      adapter: adapter,
      dir: System.get_env("TRACE_DIR", "docs/bench/traces"),
      flush_every: Bench.Env.int("TRACE_FLUSH_EVERY", 1),
      sizes_override: env_sizes("TRACE_SIZES"),
      skip_lru?: Bench.Env.truthy?("TRACE_SKIP_LRU")
    }
  end

  def run_trace(trace_id, params) do
    trace = Trace.fetch!(trace_id)
    sizes = params.sizes_override || trace.sizes

    Trace.check_file!(trace, params.dir)

    IO.puts("\n== #{trace.label} (#{trace.file}) ==\n")

    results =
      sizes
      |> Enum.with_index()
      |> Enum.map(fn {size, i} ->
        run_size(trace, size, ref_at(trace, i, params.sizes_override), params)
      end)

    {trace, results}
  end

  defp ref_at(_trace, _i, override) when not is_nil(override) do
    %{wtlfu: nil, lru: nil}
  end

  defp ref_at(trace, i, _override) do
    %{wtlfu: Enum.at(trace.wtlfu_ref, i), lru: Enum.at(trace.lru_ref, i)}
  end

  defp run_size(trace, size, refs, params) do
    name = :"replay_#{params.adapter}_#{trace.id}_#{size}"
    {cache, pid, replay_ctx} = start_replay(params.adapter, name, size)

    lru = if params.skip_lru?, do: nil, else: LRU.new(size)

    t0 = System.monotonic_time(:millisecond)
    {requests, lru} = replay(params.adapter, trace, replay_ctx, lru, params)
    elapsed_ms = System.monotonic_time(:millisecond) - t0

    stats = cache.info!(:stats)
    hit_rate = stats.hits * 100 / max(stats.hits + stats.misses, 1)

    result = %{
      size: size,
      requests: requests,
      hit_rate: hit_rate,
      lru_hit_rate: lru && LRU.hit_rate(lru),
      wtlfu_ref: refs.wtlfu,
      lru_ref: refs.lru,
      elapsed_ms: elapsed_ms
    }

    if lru do
      LRU.delete(lru)
    end

    Supervisor.stop(pid)

    lru_part =
      case result.lru_hit_rate do
        nil -> ""
        rate -> "lru=#{Float.round(rate, 2)}% "
      end

    IO.puts(
      "  size=#{size} requests=#{requests} hit_rate=#{Float.round(hit_rate, 2)}% " <>
        lru_part <> "(#{Float.round(elapsed_ms / 1_000, 1)}s)"
    )

    result
  end

  ## Adapter-specific replay setup

  defp start_replay(:tinylfu, name, size) do
    {:ok, pid} =
      Cache.start_link(
        name: name,
        max_size: size,
        buffer_opts: [
          partitions: 1,
          key_hasher: false,
          processing_interval: @never,
          drain_threshold: @threshold_off,
          drain_check_interval: @never
        ]
      )

    Cache.put_dynamic_cache(name)

    {Cache, pid, Drain.new(name)}
  end

  defp start_replay(:local, name, size) do
    # No gc/max_size options: generations rotate deterministically from the
    # replay loop, capped at `size / 2` each so worst-case resident entries
    # equal the TinyLFU run's `max_size` (capacity-comparable hit rates).
    {:ok, pid} = LocalCache.start_link(name: name)

    LocalCache.put_dynamic_cache(name)

    {LocalCache, pid, {name, max(div(size, 2), 1)}}
  end

  # Replays the trace through the TinyLFU cache (and the LRU baseline when
  # enabled), draining the maintenance pipeline synchronously every
  # `flush_every` accesses. Single chunked pass over the trace stream.
  defp replay(:tinylfu, trace, drain, lru, params) do
    trace
    |> Trace.stream(params.dir)
    |> Stream.chunk_every(params.flush_every)
    |> Enum.reduce({0, lru}, fn chunk, {count, lru} ->
      lru =
        Enum.reduce(chunk, lru, fn key, lru ->
          access(key)

          lru && LRU.access(lru, key)
        end)

      Drain.flush(drain)

      {count + length(chunk), lru}
    end)
  end

  # Replays the trace through the Local cache, rotating generations manually
  # whenever the newer generation exceeds `gen_max`.
  defp replay(:local, trace, {name, gen_max}, lru, params) do
    trace
    |> Trace.stream(params.dir)
    |> Enum.reduce({0, lru}, fn key, {count, lru} ->
      Bench.Workers.fetch_or_fill(LocalCache, key)

      if :ets.info(Generation.newer(name), :size) > gen_max do
        Generation.new(name)
      end

      {count + 1, lru && LRU.access(lru, key)}
    end)
  end

  # Cache-aside access: fetch, fill on miss. Values don't matter for hit
  # rate; store the key to keep entries small.
  defp access(key) do
    Bench.Workers.fetch_or_fill(Cache, key)
  end

  defp env_sizes(name) do
    case System.get_env(name) do
      nil -> nil
      value -> value |> String.split(",") |> Enum.map(&String.to_integer/1)
    end
  end
end

defmodule Replay.Report do
  @moduledoc false

  import Bench.Fmt, only: [md_table: 2, num: 1]

  def render(runs, params) do
    header = """
    == Trace replay ==

    Elixir #{System.version()} / OTP #{System.otp_release()}
    adapter=#{params.adapter} flush_every=#{params.flush_every} \
    lru_baseline=#{not params.skip_lru?}
    """

    sections = Enum.map_join(runs, "\n", &section(&1, params.adapter))

    header <> "\n" <> sections
  end

  defp section({trace, results}, adapter) do
    headers = ~w(size requests #{adapter} caffeine_wtlfu delta lru_sim lru_ref time)

    rows =
      Enum.map(results, fn r ->
        [
          num(r.size),
          num(r.requests),
          pct(r.hit_rate),
          pct(r.wtlfu_ref),
          delta(r.hit_rate, r.wtlfu_ref),
          pct(r.lru_hit_rate),
          pct(r.lru_ref),
          "#{Float.round(r.elapsed_ms / 1_000, 1)}s"
        ]
      end)

    """
    ### #{trace.label}

    Reference source: #{trace.ref_source}

    #{md_table(rows, headers)}
    """
  end

  defp delta(_value, nil), do: "-"
  defp delta(value, ref), do: :erlang.float_to_binary(value - ref, decimals: 2)

  defp pct(nil), do: "-"
  defp pct(value), do: Bench.Fmt.pct(value)
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

alias Replay.{Report, Runner, Trace}

params = Runner.params!()

traces =
  case System.argv() do
    [] -> Map.keys(Trace.catalog()) |> Enum.sort()
    ids -> ids
  end

runs = Enum.map(traces, &Runner.run_trace(&1, params))

Bench.Fmt.write_report!("trace_replay", Report.render(runs, params))
