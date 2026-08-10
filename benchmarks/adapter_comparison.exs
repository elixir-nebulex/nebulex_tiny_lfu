# Head-to-head comparison: Nebulex.Adapters.TinyLFU vs Nebulex.Adapters.Local.
#
# Same workloads, same nominal capacity — measures policy quality (hit rate
# under equal `max_size`), throughput (mixed ops/s from the worker loop and
# per-op get/put latency via Benchee), resident entry counts, and ETS memory.
# Results are summarized in `docs/bench/COMPARISON_RESULTS.md` (local-only).
#
# Run with:
#
#     MIX_ENV=test mix run benchmarks/adapter_comparison.exs
#
# Environment knobs (all optional):
#
#     BENCH_QUICK=1            # short smoke run, skips the Benchee part
#     BENCH_MAX_SIZE=10000     # cache max_size (both adapters)
#     BENCH_KEYSPACE=100000    # zipfian keyspace (default 10x max_size)
#     BENCH_WORKERS=6          # concurrent workload workers
#     BENCH_WARM_MS=3000       # warm phase duration
#     BENCH_TIME_MS=10000      # measured phase duration
#     BENCH_SKIP_BENCHEE=1     # skip the Benchee per-op latency part
#
# ## Fairness notes (document alongside any published numbers)
#
#   * Both caches get the same `max_size`. Local's generational model rotates
#     the newer generation into the older one when a periodic size check (set
#     to 1s here; 10s default) sees `size > max_size`, so Local's *resident*
#     entries oscillate between roughly `max_size` and `2 * max_size`. It
#     effectively holds more data than TinyLFU at the same nominal capacity,
#     and evicts in coarse generation-sized batches: accessed entries get
#     copied into the newer generation, and the rest drop with the old one.
#     The tables report measured avg/peak resident entries to keep that
#     asymmetry visible.
#   * `gc_interval` (time-based rotation) is set to 1h so rotation is purely
#     size-driven during the measured window.
#   * TinyLFU pays a buffer write per hit on the read path (the policy's
#     access event) that Local does not pay on `get`; Benchee's per-op
#     numbers make that cost visible rather than hiding it.
#   * ETS memory is sampled as the `:erlang.memory(:ets)` delta from just
#     before cache start to end of the measured phase, so it includes every
#     policy-side table (TinyLFU: data + deques + buffers; Local: the two
#     generations), not just the data table. It is a single end-of-phase
#     snapshot, and Local's resident data oscillates with the rotation
#     cycle, so the value depends on where in that cycle the phase ends.
#     Read it alongside the avg/peak entry columns, not as a precise
#     footprint.
#   * Benchee jobs use before_scenario/before_each hooks so the timed path
#     is the bare adapter call (dynamic-cache setup and key selection are
#     excluded from timing). The TinyLFU `get` job blends hit and miss paths
#     (keyspace is 10x max_size) — only the hit path pays the read-buffer
#     event write.

Code.require_file("support/helpers.exs", __DIR__)

defmodule Comp.TinyLFU do
  @moduledoc false
  use Nebulex.Cache,
    otp_app: :nebulex_tiny_lfu,
    adapter: Nebulex.Adapters.TinyLFU
end

defmodule Comp.Local do
  @moduledoc false
  use Nebulex.Cache,
    otp_app: :nebulex_tiny_lfu,
    adapter: Nebulex.Adapters.Local
end

defmodule Comp.Sampler do
  @moduledoc false
  # Samples a `size_fun` on a fixed interval; reports avg and peak.

  def start(size_fun, interval_ms) do
    spawn_link(fn -> loop(size_fun, interval_ms, %{peak: 0, sum: 0, samples: 0}) end)
  end

  def stop(pid) do
    ref = Process.monitor(pid)

    send(pid, {:stop, self()})

    receive do
      {:sampler_result, result} ->
        Process.demonitor(ref, [:flush])

        result

      {:DOWN, ^ref, :process, _pid, reason} ->
        raise "sampler died: #{inspect(reason)}"
    after
      :timer.seconds(5) -> raise "timeout waiting for sampler result"
    end
  end

  defp loop(size_fun, interval_ms, acc) do
    receive do
      {:stop, from} ->
        send(from, {:sampler_result, finalize(acc)})
    after
      interval_ms ->
        size = size_fun.()

        acc = %{
          peak: max(acc.peak, size),
          sum: acc.sum + size,
          samples: acc.samples + 1
        }

        loop(size_fun, interval_ms, acc)
    end
  end

  defp finalize(%{samples: 0} = acc) do
    %{peak: acc.peak, avg: 0}
  end

  defp finalize(acc) do
    %{peak: acc.peak, avg: div(acc.sum, acc.samples)}
  end
end

defmodule Comp.Runner do
  @moduledoc false

  alias Comp.Sampler

  # Sampling interval for the resident-entries sampler
  @sample_interval_ms 10

  # Settle time after warm-up so both adapters start the measured phase from
  # a steady state (TinyLFU: buffers drained; Local: size check ran).
  @settle_ms 2_200

  ## Parameters

  def params! do
    quick? = Bench.Env.truthy?("BENCH_QUICK")
    max_size = Bench.Env.int("BENCH_MAX_SIZE", 10_000)

    %{
      quick?: quick?,
      max_size: max_size,
      keyspace: Bench.Env.int("BENCH_KEYSPACE", max_size * 10),
      skew: 1.0,
      # Large enough that the presampled pool realizes most of the keyspace's
      # zipf tail — a small pool compresses the effective working set and
      # inflates every adapter's hit rate toward a ceiling.
      sample_size: 1_048_576,
      # Leave scheduler headroom for TinyLFU's maintenance pipeline
      workers: Bench.Env.int("BENCH_WORKERS", max(2, System.schedulers_online() - 2)),
      warm_ms: Bench.Env.int("BENCH_WARM_MS", if(quick?, do: 500, else: 3_000)),
      time_ms: Bench.Env.int("BENCH_TIME_MS", if(quick?, do: 1_500, else: 10_000)),
      skip_benchee?: quick? or Bench.Env.truthy?("BENCH_SKIP_BENCHEE"),
      benchee_time: Bench.Env.int("BENCH_BENCHEE_TIME_S", 3)
    }
  end

  ## Adapters under comparison

  def adapters(params) do
    [
      %{
        id: :tinylfu,
        cache: Comp.TinyLFU,
        label: "Nebulex.Adapters.TinyLFU (W-TinyLFU)",
        start_opts: [max_size: params.max_size]
      },
      %{
        id: :local,
        cache: Comp.Local,
        label: "Nebulex.Adapters.Local (generational)",
        start_opts: [
          max_size: params.max_size,
          # Rotation is size-driven during the run: check size every 1s,
          # never rotate on time alone.
          gc_interval: :timer.hours(1),
          gc_memory_check_interval: :timer.seconds(1)
        ]
      }
    ]
  end

  ## Scenarios (same shapes as drain_tuning.exs so numbers line up)

  def scenarios do
    [
      %{id: :zipf_read_heavy, label: "Zipfian read-heavy (95/5)", read_pct: 95},
      %{id: :write_heavy, label: "Write-heavy mixed (50/50)", read_pct: 50},
      %{id: :scan, label: "Scan (hot reads + one-shot keys)", read_pct: :scan}
    ]
  end

  ## Scenario execution

  def run_scenario(scenario, adapter, params, keys) do
    name = :"comp_#{scenario.id}_#{adapter.id}"

    IO.puts("  * #{scenario.id} / #{adapter.id} ...")

    ets_before = :erlang.memory(:ets)

    {:ok, pid} = adapter.cache.start_link([name: name] ++ adapter.start_opts)

    step = step(scenario, adapter.cache, keys)

    # Warm phase, then settle so the measured phase starts from steady
    # state. Scan warms with the 95/5 mixed step (mirrors drain_tuning) —
    # warming scan with its own step would pre-insert the measured phase's
    # {:scan, worker, i} keys, making the "one-shot" writes repeat keys.
    Bench.Workers.run(
      adapter.cache,
      name,
      params.workers,
      params.warm_ms,
      warm_step(scenario, adapter.cache, keys)
    )

    Process.sleep(@settle_ms)

    adapter.cache.put_dynamic_cache(name)

    sampler = Sampler.start(entries_fun(adapter, name), @sample_interval_ms)
    stats0 = adapter.cache.info!(:stats)

    ops = Bench.Workers.run(adapter.cache, name, params.workers, params.time_ms, step)

    stats1 = adapter.cache.info!(:stats)
    entries = Sampler.stop(sampler)
    ets_bytes = :erlang.memory(:ets) - ets_before

    # Let in-flight maintenance finish before tearing the cache down
    Process.sleep(300)
    Supervisor.stop(pid)

    %{
      scenario: scenario.id,
      adapter: adapter.id,
      hit_rate: hit_rate(stats0, stats1),
      ops: ops,
      ops_per_sec: div(ops * 1_000, max(params.time_ms, 1)),
      entries: entries,
      ets_bytes: ets_bytes
    }
  end

  # Resident entries: TinyLFU's single data table vs the sum of Local's
  # generation tables.
  defp entries_fun(%{id: :tinylfu, cache: cache}, _name) do
    data_tab = cache.data_table()

    fn -> :ets.info(data_tab, :size) end
  end

  defp entries_fun(%{id: :local}, name) do
    fn ->
      name
      |> Nebulex.Adapters.Local.Generation.list()
      |> Enum.map(&:ets.info(&1, :size))
      |> Enum.sum()
    end
  end

  defp hit_rate(stats0, stats1) do
    hits = stats1.hits - stats0.hits
    misses = stats1.misses - stats0.misses

    if hits + misses > 0, do: hits * 100 / (hits + misses), else: 0.0
  end

  ## Workload steps (cache-aside, parametrized by cache module)

  defp step(%{read_pct: :scan}, cache, keys) do
    scan_step(cache, keys)
  end

  defp step(%{read_pct: read_pct}, cache, keys) do
    mixed_step(cache, keys, read_pct)
  end

  # Hot-read percentage used to warm the scan scenario
  @scan_warm_read_pct 95

  defp warm_step(%{read_pct: :scan}, cache, keys) do
    mixed_step(cache, keys, @scan_warm_read_pct)
  end

  defp warm_step(scenario, cache, keys) do
    step(scenario, cache, keys)
  end

  # Cache-aside mixed workload: reads fill on miss
  defp mixed_step(cache, keys, read_pct) do
    nkeys = tuple_size(keys)

    fn _worker, _i ->
      key = elem(keys, :rand.uniform(nkeys) - 1)

      if :rand.uniform(100) <= read_pct do
        fetch_or_fill(cache, key)
      else
        cache.put!(key, key)
      end

      :ok
    end
  end

  # Scan workload: alternate hot zipf reads with one-shot unique-key writes
  defp scan_step(cache, keys) do
    nkeys = tuple_size(keys)

    fn worker, i ->
      if rem(i, 2) == 0 do
        fetch_or_fill(cache, elem(keys, :rand.uniform(nkeys) - 1))
      else
        cache.put!({:scan, worker, i}, i)
      end

      :ok
    end
  end

  defp fetch_or_fill(cache, key) do
    Bench.Workers.fetch_or_fill(cache, key)
  end

  ## Benchee per-op latency

  def run_benchee(adapters, params, keys) do
    IO.puts("\n== Part 2: Benchee per-op latency ==\n")

    nkeys = tuple_size(keys)

    caches =
      Enum.map(adapters, fn adapter ->
        name = :"comp_benchee_#{adapter.id}"

        {:ok, pid} = adapter.cache.start_link([name: name] ++ adapter.start_opts)

        # Reach steady state (cache full, evictions active) before measuring
        Bench.Workers.run(
          adapter.cache,
          name,
          params.workers,
          1_500,
          mixed_step(adapter.cache, keys, 50)
        )

        Process.sleep(@settle_ms)

        {adapter, name, pid}
      end)

    # Hooks keep harness work out of the timed path: the dynamic-cache pdict
    # write happens once per scenario, and the random key draw happens in
    # before_each (excluded from timing; its return value is the timed
    # function's input) — the measured op is the bare adapter call.
    jobs =
      caches
      |> Enum.flat_map(fn {adapter, name, _pid} ->
        hooks = [
          before_scenario: fn input ->
            adapter.cache.put_dynamic_cache(name)

            input
          end,
          before_each: fn _input -> elem(keys, :rand.uniform(nkeys) - 1) end
        ]

        [
          {"get (#{adapter.id})", {fn key -> adapter.cache.fetch(key) end, hooks}},
          {"put (#{adapter.id})", {fn key -> adapter.cache.put!(key, 1) end, hooks}}
        ]
      end)
      |> Map.new()

    Benchee.run(jobs,
      time: params.benchee_time,
      warmup: 1,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "benchmarks/output/comparison.html", auto_open: false}
      ]
    )
  after
    Enum.each(adapters, fn adapter ->
      case Nebulex.Cache.Registry.lookup(:"comp_benchee_#{adapter.id}") do
        {:ok, %{pid: pid}} -> Supervisor.stop(pid)
        _other -> :ok
      end
    end)
  end
end

defmodule Comp.Report do
  @moduledoc false

  import Bench.Fmt

  def header(params, adapters) do
    configs =
      Enum.map_join(adapters, "\n", fn adapter ->
        "  * #{adapter.id}: #{adapter.label} — #{inspect(adapter.start_opts)}"
      end)

    """
    == Adapter comparison bench ==

    Elixir #{System.version()} / OTP #{System.otp_release()} \
    / #{System.schedulers_online()} schedulers
    max_size=#{params.max_size} keyspace=#{params.keyspace} \
    workers=#{params.workers} warm=#{params.warm_ms}ms measure=#{params.time_ms}ms

    Adapters:
    #{configs}
    """
  end

  def render(results, params, adapters) do
    scenarios = Enum.uniq(Enum.map(results, & &1.scenario))

    sections =
      Enum.map_join(scenarios, "\n", fn scenario ->
        rows = Enum.filter(results, &(&1.scenario == scenario))

        "### #{scenario}\n\n" <> table(rows)
      end)

    header(params, adapters) <> "\n== Part 1: Policy quality & throughput ==\n\n" <> sections
  end

  defp table(rows) do
    headers = ~w(adapter hit_rate ops ops/s avg_entries peak_entries ets_mb)

    rows
    |> Enum.map(fn row ->
      [
        to_string(row.adapter),
        pct(row.hit_rate),
        num(row.ops),
        num(row.ops_per_sec),
        num(row.entries.avg),
        num(row.entries.peak),
        flt(row.ets_bytes / 1_048_576, 1)
      ]
    end)
    |> md_table(headers)
  end
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

alias Comp.{Report, Runner}

params = Runner.params!()
adapters = Runner.adapters(params)

IO.puts(Report.header(params, adapters))

keys = Bench.Zipf.presample(params.keyspace, params.skew, params.sample_size)

IO.puts("== Part 1: Policy quality & throughput ==\n")

results =
  for scenario <- Runner.scenarios(), adapter <- adapters do
    Runner.run_scenario(scenario, adapter, params, keys)
  end

unless params.skip_benchee? do
  Runner.run_benchee(adapters, params, keys)
end

Bench.Fmt.write_report!("adapter_comparison", Report.render(results, params, adapters))
