# Shared helpers for the benchmark scripts. Loaded via `Code.require_file/2`;
# guard against double-loading when several scripts run in one VM.

unless Code.ensure_loaded?(Bench.Zipf) do
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

  defmodule Bench.Env do
    @moduledoc false
    # Environment-variable knob parsing shared by the benchmark scripts.

    @doc "Reads an integer env var, falling back to `default` when unset."
    def int(name, default) do
      case System.get_env(name) do
        nil -> default
        value -> String.to_integer(value)
      end
    end

    @doc "True when the env var is set to `1` or `true`."
    def truthy?(name), do: System.get_env(name) in ~w(1 true)
  end

  defmodule Bench.Workers do
    @moduledoc false
    # Concurrent workload driver shared by the benchmark scripts.

    @doc """
    Cache-aside read against the current dynamic cache: fetch, fill on miss.
    """
    def fetch_or_fill(cache, key) do
      with {:error, _reason} <- cache.fetch(key) do
        cache.put!(key, key)
      end
    end

    @doc """
    Runs `workers` concurrent processes calling `step.(worker_index, i)` in a
    tight loop until the deadline. Each worker points `cache` at the dynamic
    cache `name` first. Returns total ops executed.
    """
    def run(cache, name, workers, duration_ms, step) do
      deadline = System.monotonic_time(:millisecond) + duration_ms

      1..workers
      |> Enum.map(fn worker ->
        Task.async(fn ->
          cache.put_dynamic_cache(name)

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
  end

  defmodule Bench.Fmt do
    @moduledoc false
    # Markdown-ish report formatting shared by the benchmark scripts.

    def md_table(rows, headers) do
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

    def pct(value), do: flt(value, 2) <> "%"

    def flt(value, decimals), do: :erlang.float_to_binary(value / 1, decimals: decimals)

    def num(value) do
      value |> Integer.to_string() |> String.replace(~r/\d(?=(\d{3})+$)/, "\\0,")
    end

    @doc """
    Writes `report` to `benchmarks/output/<prefix>-<timestamp>.md`, echoes it
    to stdout, and prints the report path.
    """
    def write_report!(prefix, report) do
      File.mkdir_p!("benchmarks/output")

      timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d-%H%M%S")
      report_file = "benchmarks/output/#{prefix}-#{timestamp}.md"

      File.write!(report_file, report)

      IO.puts("\n" <> report)
      IO.puts("Report written to #{report_file}")
    end
  end
end
