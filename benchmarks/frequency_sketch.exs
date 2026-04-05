alias Nebulex.TinyLFU.FrequencySketch

# ============================================================================
# Setup: Create sketches at different sizes and pre-populate
# ============================================================================

small_sketch = FrequencySketch.new(1_000)
medium_sketch = FrequencySketch.new(100_000)
large_sketch = FrequencySketch.new(1_000_000)

for sketch <- [small_sketch, medium_sketch, large_sketch], i <- 1..1_000 do
  FrequencySketch.increment(sketch, {:key, i})
end

# ============================================================================
# Benchmark 1: Single operation latency
# ============================================================================

IO.puts("\n=== Single Operation Latency ===\n")

inputs = %{
  "1K sketch" => {small_sketch, 1_000},
  "100K sketch" => {medium_sketch, 100_000},
  "1M sketch" => {large_sketch, 1_000_000}
}

Benchee.run(
  %{
    "increment" => {
      fn {sketch, key} -> FrequencySketch.increment(sketch, key) end,
      before_each: fn {sketch, key_space} ->
        {sketch, {:key, :rand.uniform(key_space)}}
      end
    },
    "frequency" => {
      fn {sketch, key} -> FrequencySketch.frequency(sketch, key) end,
      before_each: fn {sketch, key_space} ->
        {sketch, {:key, :rand.uniform(key_space)}}
      end
    }
  },
  inputs: inputs,
  warmup: 2,
  time: 5,
  memory_time: 2,
  print: [configuration: false]
)

# ============================================================================
# Benchmark 2: Reset cost at different sizes
# ============================================================================

IO.puts("\n=== Reset Cost ===\n")

Benchee.run(
  %{
    "reset" => {
      fn sketch -> FrequencySketch.reset(sketch) end,
      before_each: fn {max_size, populate} ->
        s = FrequencySketch.new(max_size)
        for i <- 1..populate, do: FrequencySketch.increment(s, i)
        s
      end
    }
  },
  inputs: %{
    "1K sketch" => {1_000, 500},
    "100K sketch" => {100_000, 5_000},
    "1M sketch" => {1_000_000, 50_000}
  },
  warmup: 1,
  time: 5,
  memory_time: 2,
  print: [configuration: false]
)

# ============================================================================
# Benchmark 3: Mixed workload (80% reads / 20% writes)
# ============================================================================

IO.puts("\n=== Mixed Workload (80% reads / 20% writes) ===\n")

Benchee.run(
  %{
    "mixed 80/20" => {
      fn {sketch, keys} ->
        Enum.with_index(keys, fn key, i ->
          if rem(i, 5) == 0 do
            FrequencySketch.increment(sketch, key)
          else
            FrequencySketch.frequency(sketch, key)
          end
        end)
      end,
      before_each: fn {sketch, key_space} ->
        keys = Enum.map(1..100, fn _ -> {:key, :rand.uniform(key_space)} end)
        {sketch, keys}
      end
    }
  },
  inputs: %{
    "1K sketch" => {small_sketch, 1_000},
    "100K sketch" => {medium_sketch, 100_000}
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  print: [configuration: false]
)

# ============================================================================
# Benchmark 4: Concurrent throughput
# ============================================================================

schedulers = System.schedulers_online()
IO.puts("\n=== Concurrent Throughput (#{schedulers} schedulers) ===\n")

concurrent_sketch = FrequencySketch.new(100_000)
for i <- 1..10_000, do: FrequencySketch.increment(concurrent_sketch, {:key, i})

defmodule ConcurrentBench do
  def run_concurrent(sketch, num_tasks, keys_per_task) do
    fun = fn ->
      for key <- keys_per_task do
        mix_op(sketch, key)
      end
    end

    for _ <- 1..num_tasks do
      Task.async(fun)
    end
    |> Task.await_many(:infinity)
  end

  defp mix_op(sketch, key) do
    if :rand.uniform(5) == 1 do
      FrequencySketch.increment(sketch, key)
    else
      FrequencySketch.frequency(sketch, key)
    end
  end
end

Benchee.run(
  %{
    "1 process, 1000 ops" => {
      fn {sketch, keys} -> ConcurrentBench.run_concurrent(sketch, 1, keys) end,
      before_each: fn _ ->
        keys = Enum.map(1..1_000, fn _ -> {:key, :rand.uniform(100_000)} end)
        {concurrent_sketch, keys}
      end
    },
    "#{schedulers} processes, 1000 ops each" => {
      fn {sketch, keys} ->
        ConcurrentBench.run_concurrent(sketch, schedulers, keys)
      end,
      before_each: fn _ ->
        keys = Enum.map(1..1_000, fn _ -> {:key, :rand.uniform(100_000)} end)
        {concurrent_sketch, keys}
      end
    },
    "#{schedulers * 2} processes, 1000 ops each" => {
      fn {sketch, keys} ->
        ConcurrentBench.run_concurrent(sketch, schedulers * 2, keys)
      end,
      before_each: fn _ ->
        keys = Enum.map(1..1_000, fn _ -> {:key, :rand.uniform(100_000)} end)
        {concurrent_sketch, keys}
      end
    }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  print: [configuration: false]
)
