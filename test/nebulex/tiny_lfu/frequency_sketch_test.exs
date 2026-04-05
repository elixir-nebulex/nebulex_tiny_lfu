defmodule Nebulex.TinyLFU.FrequencySketchTest do
  use ExUnit.Case, async: true
  doctest Nebulex.TinyLFU.FrequencySketch

  alias Nebulex.TinyLFU.FrequencySketch

  describe "new/1" do
    test "creates a sketch with power-of-two table length" do
      sketch = FrequencySketch.new(100)

      assert sketch.table_length == 128
      assert sketch.sample_size == 1_000
    end

    test "rounds up to nearest power of two" do
      assert FrequencySketch.new(1).table_length == 8
      assert FrequencySketch.new(7).table_length == 8
      assert FrequencySketch.new(8).table_length == 8
      assert FrequencySketch.new(9).table_length == 16
      assert FrequencySketch.new(1_000).table_length == 1024
      assert FrequencySketch.new(1_024).table_length == 1024
      assert FrequencySketch.new(1_025).table_length == 2048
    end

    test "minimum table length is 8" do
      sketch = FrequencySketch.new(1)

      assert sketch.table_length == 8
    end

    test "sample size is 10 times max_size" do
      sketch = FrequencySketch.new(500)

      assert sketch.sample_size == 5_000
    end
  end

  describe "frequency/2" do
    test "returns 0 for unseen keys" do
      sketch = FrequencySketch.new(100)

      assert FrequencySketch.frequency(sketch, "never_seen") == 0
      assert FrequencySketch.frequency(sketch, :atom_key) == 0
      assert FrequencySketch.frequency(sketch, 42) == 0
    end

    test "returns frequency after increments" do
      sketch = FrequencySketch.new(100)

      assert FrequencySketch.increment(sketch, "key") == :ok
      assert FrequencySketch.frequency(sketch, "key") == 1

      assert FrequencySketch.increment(sketch, "key") == :ok
      assert FrequencySketch.frequency(sketch, "key") == 2

      assert FrequencySketch.increment(sketch, "key") == :ok
      assert FrequencySketch.frequency(sketch, "key") == 3
    end

    test "frequency is independent per key" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..5, do: FrequencySketch.increment(sketch, :a)
      for _ <- 1..2, do: FrequencySketch.increment(sketch, :b)

      assert FrequencySketch.frequency(sketch, :a) == 5
      assert FrequencySketch.frequency(sketch, :b) == 2
      assert FrequencySketch.frequency(sketch, :c) == 0
    end

    test "works with various key types" do
      sketch = FrequencySketch.new(100)

      keys = ["string", :atom, 42, 3.14, {1, 2}, [1, 2, 3], %{a: 1}]

      for key <- keys do
        assert FrequencySketch.increment(sketch, key) == :ok
        assert FrequencySketch.frequency(sketch, key) >= 1
      end
    end
  end

  describe "increment/2" do
    test "caps counter at 15" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..20 do
        assert FrequencySketch.increment(sketch, "key") == :ok
      end

      assert FrequencySketch.frequency(sketch, "key") == 15
    end

    test "incrementing one key does not significantly affect others" do
      sketch = FrequencySketch.new(512)

      for _ <- 1..10 do
        assert FrequencySketch.increment(sketch, :target) == :ok
      end

      # Due to hash collisions in the sketch, some other keys might show
      # a small non-zero frequency, but most should be 0.
      false_positives =
        Enum.count(1..100, fn i ->
          FrequencySketch.frequency(sketch, {:other, i}) > 0
        end)

      # With a well-sized sketch, false positive rate should be low
      assert false_positives < 20
    end
  end

  describe "increment/3 (bulk count)" do
    test "default count of 1 matches increment/2" do
      a = FrequencySketch.new(100)
      b = FrequencySketch.new(100)

      for _ <- 1..7, do: FrequencySketch.increment(a, :k)
      for _ <- 1..7, do: FrequencySketch.increment(b, :k, 1)

      assert FrequencySketch.frequency(a, :k) == FrequencySketch.frequency(b, :k)
      assert FrequencySketch.frequency(a, :k) == 7
    end

    test "produces the same frequency as N successive increment/2 calls" do
      a = FrequencySketch.new(100)
      b = FrequencySketch.new(100)

      for _ <- 1..9, do: FrequencySketch.increment(a, :k)
      :ok = FrequencySketch.increment(b, :k, 9)

      assert FrequencySketch.frequency(a, :k) == FrequencySketch.frequency(b, :k)
      assert FrequencySketch.frequency(a, :k) == 9
    end

    test "saturates at 15 even for very large counts" do
      sketch = FrequencySketch.new(100)

      :ok = FrequencySketch.increment(sketch, :flooded, 1_000_000)

      assert FrequencySketch.frequency(sketch, :flooded) == 15
    end

    test "subsequent bulk increment after partial fill still saturates correctly" do
      sketch = FrequencySketch.new(100)

      :ok = FrequencySketch.increment(sketch, :k, 5)
      assert FrequencySketch.frequency(sketch, :k) == 5

      :ok = FrequencySketch.increment(sketch, :k, 100)
      assert FrequencySketch.frequency(sketch, :k) == 15
    end

    test "no-op when count is the smallest pos_integer (1) on an already-saturated key" do
      sketch = FrequencySketch.new(100)

      :ok = FrequencySketch.increment(sketch, :k, 20)
      assert FrequencySketch.frequency(sketch, :k) == 15

      :ok = FrequencySketch.increment(sketch, :k, 1)
      assert FrequencySketch.frequency(sketch, :k) == 15
    end

    test "raises on count of zero or negative" do
      sketch = FrequencySketch.new(100)

      assert_raise FunctionClauseError, fn ->
        FrequencySketch.increment(sketch, :k, 0)
      end

      assert_raise FunctionClauseError, fn ->
        FrequencySketch.increment(sketch, :k, -1)
      end
    end
  end

  describe "reset/1" do
    test "halves all counters" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..10, do: FrequencySketch.increment(sketch, :a)
      for _ <- 1..6, do: FrequencySketch.increment(sketch, :b)

      freq_a_before = FrequencySketch.frequency(sketch, :a)
      freq_b_before = FrequencySketch.frequency(sketch, :b)

      assert freq_a_before == 10
      assert freq_b_before == 6

      assert FrequencySketch.reset(sketch) == :ok

      freq_a_after = FrequencySketch.frequency(sketch, :a)
      freq_b_after = FrequencySketch.frequency(sketch, :b)

      # After halving, counters should be approximately half
      assert freq_a_after == 5
      assert freq_b_after == 3
    end

    test "reset preserves relative ordering" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..12, do: FrequencySketch.increment(sketch, :hot)
      for _ <- 1..4, do: FrequencySketch.increment(sketch, :cold)

      assert FrequencySketch.reset(sketch) == :ok

      assert FrequencySketch.frequency(sketch, :hot) > FrequencySketch.frequency(sketch, :cold)
    end

    test "automatic reset triggers when sample size is reached" do
      max_size = 64
      sketch = FrequencySketch.new(max_size)

      # Increment the same key many times to build up frequency
      for _ <- 1..15 do
        assert FrequencySketch.increment(sketch, :key) == :ok
      end

      assert FrequencySketch.frequency(sketch, :key) == 15

      # Now flood with many different keys to trigger reset
      # sample_size = 10 * 64 = 640
      for i <- 1..640 do
        assert FrequencySketch.increment(sketch, {:flood, i}) == :ok
      end

      # After reset, the frequency should have been halved
      assert FrequencySketch.frequency(sketch, :key) < 15
    end
  end

  describe "clear/1" do
    test "zeroes every counter" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..12, do: FrequencySketch.increment(sketch, :hot)
      for _ <- 1..5, do: FrequencySketch.increment(sketch, :warm)

      assert FrequencySketch.frequency(sketch, :hot) == 12
      assert FrequencySketch.frequency(sketch, :warm) == 5

      assert FrequencySketch.clear(sketch) == :ok

      assert FrequencySketch.frequency(sketch, :hot) == 0
      assert FrequencySketch.frequency(sketch, :warm) == 0
    end

    test "leaves the sketch usable for subsequent increments" do
      sketch = FrequencySketch.new(100)

      for _ <- 1..8, do: FrequencySketch.increment(sketch, :a)
      :ok = FrequencySketch.clear(sketch)

      for _ <- 1..3, do: FrequencySketch.increment(sketch, :a)

      assert FrequencySketch.frequency(sketch, :a) == 3
    end

    test "resets the internal sample counter (no premature aging after clear)" do
      max_size = 64
      sketch = FrequencySketch.new(max_size)

      # Build up close to the sample threshold (sample_size = 10 * 64 = 640).
      for i <- 1..600, do: FrequencySketch.increment(sketch, {:warmup, i})

      :ok = FrequencySketch.clear(sketch)

      # If clear didn't zero :size, this loop would trigger an automatic
      # reset after ~40 more increments and halve the new entry's count.
      for _ <- 1..15, do: FrequencySketch.increment(sketch, :fresh)

      assert FrequencySketch.frequency(sketch, :fresh) == 15
    end
  end

  describe "concurrent access" do
    test "handles concurrent increments without crashing" do
      sketch = FrequencySketch.new(1_000)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            for j <- 1..100 do
              key = rem(j, 20)
              :ok = FrequencySketch.increment(sketch, {:key, key})

              if rem(j + i, 7) == 0 do
                FrequencySketch.frequency(sketch, {:key, key})
              end
            end
          end)
        end

      Task.await_many(tasks)

      # Just verify we can read frequencies without error
      for k <- 0..19 do
        freq = FrequencySketch.frequency(sketch, {:key, k})

        assert freq >= 0 and freq <= 15
      end
    end

    test "concurrent increments produce reasonable frequencies" do
      sketch = FrequencySketch.new(1_000)

      # 10 processes each increment :hot 50 times = 500 total increments
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for _ <- 1..50 do
              :ok = FrequencySketch.increment(sketch, :hot)
            end
          end)
        end

      Task.await_many(tasks)

      # Due to races, frequency may not be exactly 500 (capped at 15 anyway),
      # but should be at maximum (15) given 500 increments
      assert FrequencySketch.frequency(sketch, :hot) == 15
    end
  end

  describe "accuracy" do
    test "frequency ordering matches actual access frequency" do
      sketch = FrequencySketch.new(512)

      # Create keys with known frequency distribution
      for _ <- 1..15, do: FrequencySketch.increment(sketch, :very_hot)
      for _ <- 1..10, do: FrequencySketch.increment(sketch, :hot)
      for _ <- 1..5, do: FrequencySketch.increment(sketch, :warm)
      for _ <- 1..1, do: FrequencySketch.increment(sketch, :cold)

      freq_very_hot = FrequencySketch.frequency(sketch, :very_hot)
      freq_hot = FrequencySketch.frequency(sketch, :hot)
      freq_warm = FrequencySketch.frequency(sketch, :warm)
      freq_cold = FrequencySketch.frequency(sketch, :cold)

      assert freq_very_hot >= freq_hot
      assert freq_hot >= freq_warm
      assert freq_warm >= freq_cold
    end
  end
end
