# Drain-tuning benchmark for the buffer/maintenance pipeline.
#
# Measures policy-side outcomes (hit rate, `max_size` overshoot, policy lag,
# maintenance churn) across drain configurations, plus a Benchee guardrail
# confirming the hot path doesn't regress. See
# `docs/plans/2026-08-09-001-feat-drain-tuning-bench-plan.md` for the design.
#
# Run with:
#
#     MIX_ENV=test mix run benchmarks/drain_tuning.exs
#
# Environment knobs (all optional):
#
#     BENCH_QUICK=1            # short smoke run, skips the Benchee guardrail
#     BENCH_MAX_SIZE=10000     # cache max_size
#     BENCH_KEYSPACE=100000    # zipfian keyspace (default 10x max_size)
#     BENCH_WORKERS=6          # concurrent workload workers
#     BENCH_WARM_MS=3000       # warm phase duration
#     BENCH_TIME_MS=10000      # measured phase duration
#     BENCH_INTERVAL_MS=1000   # buffer processing_interval
#     BENCH_SKIP_GUARDRAIL=1   # skip the Benchee part
#
# Config grid (per-buffer drain settings applied via
# `Tidefall.Buffer.update_options/2`, since the adapter's public
# `:buffer_opts` applies one set of options to all buffers):
#
#   * derived       — the adapter's shipped derived defaults, untouched
#   * interval_only — thresholds effectively disabled (pre-defaults behavior)
#   * conservative  — derived thresholds, queue: processing_batch_size/2
#   * no_read_thr   — write + queue thresholds only, read interval-only
#
# Relative deltas between configs are the signal, not absolute numbers.

defmodule Bench.Cache do
  @moduledoc false
  use Nebulex.Cache,
    otp_app: :nebulex_tiny_lfu,
    adapter: Nebulex.Adapters.TinyLFU
end

defmodule Bench.Zipf do
  @moduledoc false
  # Zipfian key sampling via inverse CDF. Builds the cumulative weights once,
  # presamples a large pool of keys, and workloads then index uniformly into
  # the pool — preserving the zipf frequency profile with O(1) per-op cost.

  @doc "Presamples `sample_size` zipf-distributed keys from `1..keyspace`."
  def presample(keyspace, skew, sample_size) do
    cdf = build_cdf(keyspace, skew)
    total = elem(cdf, keyspace - 1)

    1..sample_size
    |> Enum.map(fn _ -> bsearch(cdf, :rand.uniform() * total, 0, keyspace - 1) + 1 end)
    |> List.to_tuple()
  end

  defp build_cdf(keyspace, skew) do
    {cumulative, _acc} =
      Enum.map_reduce(1..keyspace, 0.0, fn rank, acc ->
        acc = acc + 1.0 / :math.pow(rank, skew)

        {acc, acc}
      end)

    List.to_tuple(cumulative)
  end

  # Smallest index whose cumulative weight covers `r`
  defp bsearch(cdf, r, lo, hi) when lo < hi do
    mid = div(lo + hi, 2)

    if elem(cdf, mid) < r do
      bsearch(cdf, r, mid + 1, hi)
    else
      bsearch(cdf, r, lo, mid)
    end
  end

  defp bsearch(_cdf, _r, lo, _hi) do
    lo
  end
end

defmodule Bench.Churn do
  @moduledoc false
  # Counts Tidefall drain events per buffer role via telemetry. Tidefall wraps
  # each drain in a `:telemetry.span/3` — the `[:tidefall, :partition,
  # :processing, :stop]` event carries a `:size` measurement (entries drained)
  # and the buffer name in metadata. Empty tables never spawn a drain task, so
  # the event count is true churn.

  @event [:tidefall, :partition, :processing, :stop]
  @roles [:read, :write, :queue]

  def new_table do
    :ets.new(:bench_churn, [:set, :public, {:write_concurrency, true}])
  end

  def attach(tab, roles) do
    id = "bench-churn-#{System.unique_integer([:positive])}"

    :ok = :telemetry.attach(id, @event, &__MODULE__.handle_event/4, %{tab: tab, roles: roles})

    id
  end

  def detach(id), do: :telemetry.detach(id)

  def handle_event(@event, measurements, %{buffer: buffer}, %{tab: tab, roles: roles}) do
    case roles do
      %{^buffer => role} ->
        size = Map.get(measurements, :size, 0)

        :ets.update_counter(tab, {role, :drains}, 1, {{role, :drains}, 0})
        :ets.update_counter(tab, {role, :items}, size, {{role, :items}, 0})

        :ok

      _other ->
        :ok
    end
  end

  def reset(tab), do: :ets.delete_all_objects(tab)

  def snapshot(tab) do
    Map.new(@roles, fn role ->
      {role, %{drains: counter(tab, {role, :drains}), items: counter(tab, {role, :items})}}
    end)
  end

  defp counter(tab, key) do
    case :ets.lookup(tab, key) do
      [{^key, count}] -> count
      [] -> 0
    end
  end
end

defmodule Bench.Sampler do
  @moduledoc false
  # Samples `:ets.info(data_tab, :size)` on a fixed interval to capture
  # `max_size` overshoot: peak size, fraction of time over the cap, and the
  # mean excess over the cap (per-sample mean ~= time-weighted at a fixed
  # interval).

  def start(data_tab, max_size, interval_ms) do
    acc = %{peak: 0, samples: 0, over: 0, excess_sum: 0}

    spawn_link(fn -> loop(data_tab, max_size, interval_ms, acc) end)
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
      5_000 -> raise "timeout waiting for sampler result"
    end
  end

  defp loop(data_tab, max_size, interval_ms, acc) do
    receive do
      {:stop, from} ->
        send(from, {:sampler_result, finalize(acc)})
    after
      interval_ms ->
        size = :ets.info(data_tab, :size)

        acc = %{
          peak: max(acc.peak, size),
          samples: acc.samples + 1,
          over: acc.over + if(size > max_size, do: 1, else: 0),
          excess_sum: acc.excess_sum + max(0, size - max_size)
        }

        loop(data_tab, max_size, interval_ms, acc)
    end
  end

  defp finalize(%{samples: 0} = acc) do
    %{peak: acc.peak, over_pct: 0.0, avg_excess: 0.0}
  end

  defp finalize(acc) do
    %{
      peak: acc.peak,
      over_pct: acc.over * 100 / acc.samples,
      avg_excess: acc.excess_sum / acc.samples
    }
  end
end

defmodule Bench.Runner do
  @moduledoc false

  alias Bench.{Cache, Churn, Sampler}
  alias Nebulex.TinyLFU.Maintenance

  ## Parameters

  def params! do
    quick? = truthy?("BENCH_QUICK")
    interval = env_int("BENCH_INTERVAL_MS", 1_000)
    max_size = env_int("BENCH_MAX_SIZE", 10_000)

    %{
      quick?: quick?,
      interval: interval,
      max_size: max_size,
      keyspace: env_int("BENCH_KEYSPACE", max_size * 10),
      skew: 1.0,
      sample_size: 131_072,
      # Leave scheduler headroom for the maintenance pipeline itself
      workers: env_int("BENCH_WORKERS", max(2, System.schedulers_online() - 2)),
      warm_ms: env_int("BENCH_WARM_MS", if(quick?, do: 500, else: 3_000)),
      time_ms: env_int("BENCH_TIME_MS", if(quick?, do: 1_500, else: 10_000)),
      burst_factor: 5,
      probe_n: 10_000,
      sample_interval: 10,
      partitions: System.schedulers_online(),
      batch_size: 100,
      skip_guardrail?: quick? or truthy?("BENCH_SKIP_GUARDRAIL"),
      guardrail_time: env_int("BENCH_GUARDRAIL_TIME_S", 3)
    }
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp truthy?(name), do: System.get_env(name) in ~w(1 true)

  ## Config grid

  def configs(%{max_size: max_size, interval: interval} = params) do
    # Build the grid from the adapter's real derivation so the bench can't
    # drift out of sync with the shipped defaults.
    derived =
      Maintenance.Supervisor.derive_drain_opts([processing_interval: interval], max_size)

    check = derived.write[:drain_check_interval]
    write_t = derived.write[:drain_threshold]
    read_t = derived.read[:drain_threshold]

    # A threshold too large to ever trip, checked once an hour — emulates
    # interval-only draining (thresholds can't be unset at runtime).
    off = {1_000_000_000, 3_600_000}

    [
      # The adapter's shipped derived defaults (write: min(128, max/parts),
      # read: 64, queue: 1, check: interval/10) — no runtime overrides.
      %{id: :derived, drain: nil},
      # Interval-only draining (the pre-defaults behavior).
      %{id: :interval_only, drain: [read: off, write: off, queue: off]},
      # Derived thresholds but a lazier queue (batch_size/2 instead of 1).
      %{
        id: :conservative,
        drain: [
          read: {read_t, check},
          write: {write_t, check},
          queue: {div(params.batch_size, 2), check}
        ]
      },
      # Isolates the read-buffer threshold: write + queue thresholds on,
      # read buffer interval-only.
      %{
        id: :no_read_thr,
        drain: [read: off, write: {write_t, check}, queue: {1, check}]
      }
    ]
  end

  ## Scenarios

  def scenarios do
    [
      %{id: :zipf_read_heavy, label: "Zipfian read-heavy (95/5)", type: :mixed, read_pct: 95},
      %{id: :write_heavy, label: "Write-heavy mixed (50/50)", type: :mixed, read_pct: 50},
      %{id: :burst, label: "High-cardinality burst", type: :burst},
      %{id: :scan, label: "Scan (hot reads + one-shot keys)", type: :scan}
    ]
  end

  ## Scenario execution

  def run_scenario(%{type: :mixed, read_pct: read_pct} = scenario, config, params, keys) do
    with_cache(scenario, config, params, fn ctx ->
      step = mixed_step(keys, read_pct)

      warm(ctx, params, step)

      {sampler, stats0} = start_measurement(ctx, params)
      ops = run_workers(ctx.name, params.workers, params.time_ms, step)
      {samp, churn, hit_rate} = finish_measurement(ctx, params, sampler, stats0)

      base_result(scenario, config, params.time_ms, samp, churn)
      |> Map.merge(%{hit_rate: hit_rate, ops: ops})
    end)
  end

  def run_scenario(%{type: :burst} = scenario, config, params, keys) do
    with_cache(scenario, config, params, fn ctx ->
      warm(ctx, params, mixed_step(keys, 95))

      {sampler, _stats0} = start_measurement(ctx, params)

      # Flood: unique keys, fixed count, as fast as possible
      keys_per_worker = div(params.burst_factor * params.max_size, params.workers)
      t0 = System.monotonic_time(:millisecond)
      run_flood(ctx.name, params.workers, keys_per_worker)
      flood_ms = System.monotonic_time(:millisecond) - t0

      # Policy lag: time until ETS size converges back under max_size
      lag = await_convergence(ctx.data_tab, params.max_size, 30_000)

      # Hot-set retention: read-only zipf probe (no fill on miss)
      retention = probe_retention(keys, params.probe_n)

      samp = Sampler.stop(sampler)
      churn = Churn.snapshot(ctx.churn_tab)

      base_result(scenario, config, flood_ms, samp, churn)
      |> Map.merge(%{flood_ms: flood_ms, lag: lag, retention: retention})
    end)
  end

  def run_scenario(%{type: :scan} = scenario, config, params, keys) do
    with_cache(scenario, config, params, fn ctx ->
      warm(ctx, params, mixed_step(keys, 95))

      {sampler, stats0} = start_measurement(ctx, params)
      ops = run_workers(ctx.name, params.workers, params.time_ms, scan_step(keys))
      {samp, churn, hit_rate} = finish_measurement(ctx, params, sampler, stats0)

      base_result(scenario, config, params.time_ms, samp, churn)
      |> Map.merge(%{hit_rate: hit_rate, ops: ops})
    end)
  end

  defp base_result(scenario, config, dur_ms, samp, churn) do
    %{scenario: scenario.id, config: config.id, dur_ms: dur_ms, sampler: samp, churn: churn}
  end

  ## Cache lifecycle

  def start_cache(name, params) do
    {:ok, pid} =
      Cache.start_link(
        name: name,
        max_size: params.max_size,
        buffer_opts: [
          processing_interval: params.interval,
          processing_batch_size: params.batch_size
        ]
      )

    Cache.put_dynamic_cache(name)

    pid
  end

  defp with_cache(scenario, config, params, fun) do
    name = :"drain_bench_#{scenario.id}_#{config.id}"
    pid = start_cache(name, params)

    apply_drain(name, config)

    churn_tab = Churn.new_table()
    handler = Churn.attach(churn_tab, buffer_roles(name))

    ctx = %{name: name, data_tab: Cache.data_table(), churn_tab: churn_tab}

    IO.puts("  * #{scenario.id} / #{config.id} ...")

    try do
      fun.(ctx)
    after
      Churn.detach(handler)
      :ets.delete(churn_tab)

      # Quiesce: let in-flight drain tasks finish before tearing the cache
      # down, so shutdown doesn't race the maintenance pipeline.
      Process.sleep(300)
      Supervisor.stop(pid)
    end
  end

  def apply_drain(_name, %{drain: nil}) do
    :ok
  end

  def apply_drain(name, %{drain: drain}) do
    Enum.each(drain, fn {role, {threshold, check_interval}} ->
      name
      |> buffer_name(role)
      |> Tidefall.Buffer.update_options(
        drain_threshold: threshold,
        drain_check_interval: check_interval
      )
    end)
  end

  defp buffer_name(name, :read), do: Maintenance.read_buffer_name(name)
  defp buffer_name(name, :write), do: Maintenance.write_buffer_name(name)
  defp buffer_name(name, :queue), do: Maintenance.queue_name(name)

  defp buffer_roles(name) do
    %{
      Maintenance.read_buffer_name(name) => :read,
      Maintenance.write_buffer_name(name) => :write,
      Maintenance.queue_name(name) => :queue
    }
  end

  ## Measurement phases

  # Warm the cache, then let the pipeline settle so every config starts the
  # measured phase from a comparable steady state.
  defp warm(ctx, params, step) do
    run_workers(ctx.name, params.workers, params.warm_ms, step)

    Process.sleep(params.interval * 2 + 200)
  end

  defp start_measurement(ctx, params) do
    Churn.reset(ctx.churn_tab)

    sampler = Sampler.start(ctx.data_tab, params.max_size, params.sample_interval)
    stats0 = Cache.info!(:stats)

    {sampler, stats0}
  end

  defp finish_measurement(ctx, _params, sampler, stats0) do
    stats1 = Cache.info!(:stats)
    samp = Sampler.stop(sampler)
    churn = Churn.snapshot(ctx.churn_tab)

    {samp, churn, hit_rate(stats0, stats1)}
  end

  defp hit_rate(stats0, stats1) do
    hits = stats1.hits - stats0.hits
    misses = stats1.misses - stats0.misses

    if hits + misses > 0, do: hits * 100 / (hits + misses), else: 0.0
  end

  ## Workload steps

  # Cache-aside mixed workload: reads fill on miss (the canonical pattern, and
  # what keeps hit rate a policy-quality signal rather than a one-way decay).
  def mixed_step(keys, read_pct) do
    nkeys = tuple_size(keys)

    fn _worker, _i ->
      key = elem(keys, :rand.uniform(nkeys) - 1)

      if :rand.uniform(100) <= read_pct do
        fetch_or_fill(key)
      else
        Cache.put!(key, key)
      end

      :ok
    end
  end

  # Scan workload: alternate hot zipf reads with one-shot unique-key writes.
  # Only the hot reads touch fetch, so the global hit rate isolates hot-set
  # retention under scan pressure.
  def scan_step(keys) do
    nkeys = tuple_size(keys)

    fn worker, i ->
      if rem(i, 2) == 0 do
        fetch_or_fill(elem(keys, :rand.uniform(nkeys) - 1))
      else
        Cache.put!({:scan, worker, i}, i)
      end

      :ok
    end
  end

  # Cache-aside read: fill on miss
  defp fetch_or_fill(key) do
    with {:error, _reason} <- Cache.fetch(key) do
      Cache.put!(key, key)
    end
  end

  ## Workers

  # Runs `workers` concurrent processes calling `step.(worker_index, i)` in a
  # tight loop until the deadline. Returns total ops executed.
  def run_workers(name, workers, duration_ms, step) do
    deadline = System.monotonic_time(:millisecond) + duration_ms

    1..workers
    |> Enum.map(fn worker ->
      Task.async(fn ->
        Cache.put_dynamic_cache(name)

        worker_loop(worker, deadline, step, 0)
      end)
    end)
    |> Task.await_many(:infinity)
    |> Enum.sum()
  end

  defp worker_loop(worker, deadline, step, i) do
    if System.monotonic_time(:millisecond) >= deadline do
      i
    else
      step.(worker, i)

      worker_loop(worker, deadline, step, i + 1)
    end
  end

  defp run_flood(name, workers, keys_per_worker) do
    1..workers
    |> Enum.map(fn worker ->
      Task.async(fn ->
        Cache.put_dynamic_cache(name)

        Enum.each(1..keys_per_worker, &Cache.put!({:burst, worker, &1}, &1))
      end)
    end)
    |> Task.await_many(:infinity)
  end

  defp await_convergence(data_tab, max_size, timeout_ms) do
    t0 = System.monotonic_time(:millisecond)

    # Small tolerance over max_size: with the default `key_hasher: true`, a
    # handful of phash2 collisions among the flood's unique keys leave
    # entries untracked by the policy (documented `:key_hasher` caveat), so
    # ETS settles slightly above max_size and would never "converge" exactly.
    tolerance = max(16, div(max_size, 500))

    do_await_convergence(data_tab, max_size + tolerance, t0 + timeout_ms, t0)
  end

  defp do_await_convergence(data_tab, target, deadline, t0) do
    cond do
      :ets.info(data_tab, :size) <= target ->
        {:ok, System.monotonic_time(:millisecond) - t0}

      System.monotonic_time(:millisecond) >= deadline ->
        {:timeout, :ets.info(data_tab, :size)}

      true ->
        Process.sleep(5)

        do_await_convergence(data_tab, target, deadline, t0)
    end
  end

  # Read-only probe (no fill on miss): what fraction of a fresh zipf sample
  # is still cached after the burst?
  defp probe_retention(keys, probe_n) do
    nkeys = tuple_size(keys)

    hits =
      Enum.count(1..probe_n, fn _i ->
        match?({:ok, _}, Cache.fetch(elem(keys, :rand.uniform(nkeys) - 1)))
      end)

    hits * 100 / probe_n
  end

  ## Benchee guardrail

  def run_guardrail(configs, params, keys) do
    IO.puts("\n== Part 1: Benchee hot-path guardrail ==\n")

    nkeys = tuple_size(keys)

    caches =
      Enum.map(configs, fn config ->
        name = :"drain_bench_guardrail_#{config.id}"
        pid = start_cache(name, params)

        apply_drain(name, config)

        # Reach steady state (cache full, evictions active) before measuring
        run_workers(name, params.workers, 1_500, mixed_step(keys, 50))
        Process.sleep(params.interval * 2 + 200)

        {config.id, name, pid}
      end)

    jobs =
      caches
      |> Enum.flat_map(fn {id, name, _pid} ->
        [
          {"get (#{id})",
           fn ->
             Bench.Cache.put_dynamic_cache(name)
             Bench.Cache.fetch(elem(keys, :rand.uniform(nkeys) - 1))
           end},
          {"put (#{id})",
           fn ->
             Bench.Cache.put_dynamic_cache(name)
             Bench.Cache.put!(elem(keys, :rand.uniform(nkeys) - 1), 1)
           end}
        ]
      end)
      |> Map.new()

    Benchee.run(jobs,
      time: params.guardrail_time,
      warmup: 1,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "benchmarks/output/guardrail.html", auto_open: false}
      ]
    )
  after
    Enum.each(configs, fn config ->
      name = :"drain_bench_guardrail_#{config.id}"

      case Nebulex.Cache.Registry.lookup(name) do
        {:ok, %{pid: pid}} -> Supervisor.stop(pid)
        _other -> :ok
      end
    end)
  end
end

defmodule Bench.Report do
  @moduledoc false

  def print_header(params, configs) do
    drains =
      Enum.map_join(configs, "\n", fn
        %{id: id, drain: nil} ->
          "  * #{id}: drain thresholds unset (interval-only)"

        %{id: id, drain: drain} ->
          settings =
            Enum.map_join(drain, ", ", fn {role, {threshold, check}} ->
              "#{role}: #{threshold}/#{check}ms"
            end)

          "  * #{id}: #{settings}"
      end)

    """
    == Drain tuning bench ==

    Elixir #{System.version()} / OTP #{System.otp_release()} \
    / #{System.schedulers_online()} schedulers
    max_size=#{params.max_size} keyspace=#{params.keyspace} \
    workers=#{params.workers} partitions=#{params.partitions}
    processing_interval=#{params.interval}ms batch_size=#{params.batch_size} \
    warm=#{params.warm_ms}ms measure=#{params.time_ms}ms

    Config grid (drain_threshold/drain_check_interval per buffer):
    #{drains}
    """
  end

  def render(results, params, configs) do
    scenarios = Enum.uniq(Enum.map(results, & &1.scenario))

    sections =
      Enum.map_join(scenarios, "\n", fn scenario ->
        rows = Enum.filter(results, &(&1.scenario == scenario))

        "### #{scenario}\n\n" <> table(rows)
      end)

    print_header(params, configs) <> "\n== Part 2: Policy-side scenarios ==\n\n" <> sections
  end

  defp table([%{flood_ms: _} | _] = rows) do
    headers =
      ~w(config peak_size avg_excess flood_ms converge_ms retention drains_r/w/q batch_r/w/q)

    rows
    |> Enum.map(fn row ->
      [
        to_string(row.config),
        num(row.sampler.peak),
        flt(row.sampler.avg_excess, 1),
        num(row.flood_ms),
        lag(row.lag),
        pct(row.retention),
        churn_drains(row.churn),
        churn_batches(row.churn)
      ]
    end)
    |> md_table(headers)
  end

  defp table(rows) do
    headers = ~w(config hit_rate ops ops/s peak_size time>max avg_excess drains_r/w/q batch_r/w/q)

    rows
    |> Enum.map(fn row ->
      [
        to_string(row.config),
        pct(row.hit_rate),
        num(row.ops),
        num(div(row.ops * 1_000, max(row.dur_ms, 1))),
        num(row.sampler.peak),
        pct(row.sampler.over_pct),
        flt(row.sampler.avg_excess, 1),
        churn_drains(row.churn),
        churn_batches(row.churn)
      ]
    end)
    |> md_table(headers)
  end

  defp churn_drains(churn) do
    Enum.map_join([:read, :write, :queue], "/", &num(churn[&1].drains))
  end

  defp churn_batches(churn) do
    Enum.map_join([:read, :write, :queue], "/", fn role ->
      %{drains: drains, items: items} = churn[role]

      if drains > 0, do: num(div(items, drains)), else: "-"
    end)
  end

  defp lag({:ok, ms}), do: num(ms)
  defp lag({:timeout, size}), do: "timeout (size=#{num(size)})"

  defp md_table(rows, headers) do
    widths =
      [headers | rows]
      |> Enum.zip_with(fn cells -> cells |> Enum.map(&String.length/1) |> Enum.max() end)

    separator = Enum.map(widths, &String.duplicate("-", &1))

    [headers, separator | rows]
    |> Enum.map_join("\n", fn cells ->
      cells
      |> Enum.zip_with(widths, &String.pad_trailing/2)
      |> Enum.join(" | ")
      |> then(&("| " <> &1 <> " |"))
    end)
    |> Kernel.<>("\n")
  end

  defp pct(value), do: flt(value, 2) <> "%"

  defp flt(value, decimals), do: :erlang.float_to_binary(value / 1, decimals: decimals)

  defp num(value), do: value |> Integer.to_string() |> String.replace(~r/\d(?=(\d{3})+$)/, "\\0,")
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

alias Bench.{Report, Runner, Zipf}

params = Runner.params!()
configs = Runner.configs(params)

IO.puts(Report.print_header(params, configs))

keys = Zipf.presample(params.keyspace, params.skew, params.sample_size)

unless params.skip_guardrail? do
  Runner.run_guardrail(configs, params, keys)
end

IO.puts("\n== Part 2: Policy-side scenarios ==\n")

results =
  for scenario <- Runner.scenarios(), config <- configs do
    Runner.run_scenario(scenario, config, params, keys)
  end

report = Report.render(results, params, configs)

File.mkdir_p!("benchmarks/output")

timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d-%H%M%S")
report_file = "benchmarks/output/drain_tuning-#{timestamp}.md"

File.write!(report_file, report)

IO.puts("\n" <> report)
IO.puts("Report written to #{report_file}")
