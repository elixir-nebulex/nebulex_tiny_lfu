defmodule Nebulex.TinyLFU.FrequencySketch do
  @moduledoc """
  A probabilistic multiset for estimating the popularity of an element within
  a time window. The maximum frequency of an element is limited to 15 (4-bits)
  and an aging process periodically halves the popularity of all elements.

  This is an Elixir port of Caffeine's `FrequencySketch`, which implements a
  4-bit Count-Min Sketch with periodic aging for the TinyLFU admission policy.

  ## Implementation Details

  The counter matrix is stored in an `:atomics` array of unsigned 64-bit
  integers, each holding 16 x 4-bit counters. A fixed depth of four hash
  functions balances accuracy and cost, resulting in a confidence of 93.75%.

  Items are distributed to blocks of 8 elements for locality. Each counter
  for an item is selected from a distinct pair within the block. The frequency
  of all entries is aged periodically using a sampling window based on the
  maximum number of entries in the cache (the "reset" operation divides all
  counters by two).

  Memory usage is approximately 8 bytes per cache entry.

  ## References

    * [TinyLFU: A Highly Efficient Cache Admission Policy](https://dl.acm.org/citation.cfm?id=3149371)
    * [An Improved Data Stream Summary: The Count-Min Sketch and its Applications](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)
    * Caffeine's `FrequencySketch.java` by Ben Manes

  """

  import Bitwise

  @enforce_keys [:table, :size, :table_length, :sample_size, :block_mask]
  defstruct [:table, :size, :table_length, :sample_size, :block_mask]

  @typedoc "FrequencySketch struct"
  @type t() :: %__MODULE__{
          table: :atomics.atomics_ref(),
          size: :atomics.atomics_ref(),
          table_length: pos_integer(),
          sample_size: pos_integer(),
          block_mask: non_neg_integer()
        }

  # Mask to preserve 4-bit counter boundaries after right shift.
  # Each 4-bit nibble has its high bit cleared: 0x7 = 0b0111.
  @reset_mask 0x7777777777777777

  # Mask with the lowest bit of each 4-bit counter set.
  # Used during reset to count odd counters.
  @one_mask 0x1111111111111111

  # Maximum value for a 4-bit counter.
  @counter_mask 0xF

  # 32-bit mask for integer overflow emulation.
  @int32_mask 0xFFFFFFFF

  # Minimum table length (must be at least 8 for block-based layout).
  @min_table_length 8

  ## API

  @doc """
  Creates a new frequency sketch sized for the given maximum number of entries.

  The table length is rounded up to the nearest power of two (minimum 8),
  and the sample size (reset threshold) is set to 10 times the maximum.

  ## Examples

      iex> sketch = Nebulex.TinyLFU.FrequencySketch.new(1_000)
      iex> sketch.table_length
      1024
      iex> sketch.sample_size
      10_000

  """
  @spec new(pos_integer()) :: t()
  def new(max_size) when is_integer(max_size) and max_size > 0 do
    table_length = max(@min_table_length, ceiling_power_of_two(max_size))
    sample_size = 10 * max_size

    %__MODULE__{
      table: :atomics.new(table_length, signed: false),
      size: :atomics.new(1, signed: false),
      table_length: table_length,
      sample_size: sample_size,
      block_mask: (table_length >>> 3) - 1
    }
  end

  @doc """
  Returns the estimated number of occurrences of `key`, up to a maximum of 15.

  The estimate is the minimum of the 4 counters associated with the key
  in the Count-Min Sketch.

  ## Examples

      iex> sketch = Nebulex.TinyLFU.FrequencySketch.new(100)
      iex> Nebulex.TinyLFU.FrequencySketch.frequency(sketch, "key")
      0

  """
  @spec frequency(t(), any()) :: 0..15
  def frequency(%__MODULE__{} = sketch, key) do
    hash = hash(key)
    block_hash = spread(hash)
    counter_hash = rehash(block_hash)

    # Each block contains 8 table slots; <<< 3 converts block index to slot offset
    block = (block_hash &&& sketch.block_mask) <<< 3

    Enum.reduce(0..3, 15, fn i, min_freq ->
      # Extract the i-th byte from counter_hash (each byte selects one counter)
      h = counter_hash >>> (i <<< 3) &&& 0xFF

      # Bits [1..4] select which of the 16 counters within the 64-bit slot
      index = h >>> 1 &&& 15

      # Bit [0] selects even/odd slot within the pair; (i <<< 1) offsets to the pair
      slot = block + (h &&& 1) + (i <<< 1)
      count = counter_at(sketch.table, slot, index)

      min(min_freq, count)
    end)
  end

  @doc """
  Increments the popularity of `key` `count` times (default `1`).

  Each of the four hash cells for the key is incremented by
  `min(count, 15 - current_cell_value)` — i.e. as many of the requested
  increments as fit before the 4-bit counter saturates at 15. Triggers a
  reset (aging) when the cumulative increment count reaches the sample
  size threshold.

  Passing a `count > 1` is equivalent to calling `increment/2` that many
  times, but does it with a single read+write per cell instead of N. The
  intended use is the maintenance worker replaying the buffer's
  `updates` counter for a coalesced hot key — see
  `Nebulex.TinyLFU.Maintenance`.

  ## Examples

      iex> sketch = Nebulex.TinyLFU.FrequencySketch.new(100)
      iex> Nebulex.TinyLFU.FrequencySketch.increment(sketch, "key")
      :ok
      iex> Nebulex.TinyLFU.FrequencySketch.frequency(sketch, "key")
      1

      iex> sketch = Nebulex.TinyLFU.FrequencySketch.new(100)
      iex> Nebulex.TinyLFU.FrequencySketch.increment(sketch, "hot", 7)
      :ok
      iex> Nebulex.TinyLFU.FrequencySketch.frequency(sketch, "hot")
      7

  """
  @spec increment(t(), any(), pos_integer()) :: :ok
  def increment(sketch, key, count \\ 1)

  def increment(%__MODULE__{} = sketch, key, count)
      when is_integer(count) and count > 0 do
    hash = hash(key)
    block_hash = spread(hash)
    counter_hash = rehash(block_hash)

    # Each block contains 8 table slots; <<< 3 converts block index to slot offset
    block = (block_hash &&& sketch.block_mask) <<< 3

    # Unrolled loop over 4 hash functions (depth = 4).
    # Each byte of counter_hash drives one hash function:
    #   - Bit [0]: selects even/odd slot within the pair
    #   - Bits [1..4]: selects which of the 16 counters in the 64-bit slot
    #   - Offset (+0, +2, +4, +6): each hash function uses a distinct pair of slots
    h0 = counter_hash &&& 0xFF
    h1 = counter_hash >>> 8 &&& 0xFF
    h2 = counter_hash >>> 16 &&& 0xFF
    h3 = counter_hash >>> 24 &&& 0xFF

    added0 = increment_at(sketch.table, block + (h0 &&& 1), h0 >>> 1 &&& 15, count)
    added1 = increment_at(sketch.table, block + (h1 &&& 1) + 2, h1 >>> 1 &&& 15, count)
    added2 = increment_at(sketch.table, block + (h2 &&& 1) + 4, h2 >>> 1 &&& 15, count)
    added3 = increment_at(sketch.table, block + (h3 &&& 1) + 6, h3 >>> 1 &&& 15, count)

    # The size counter ticks by the number of iterations of an equivalent
    # call-N-times loop that would have actually incremented something.
    # Cells saturate at different rates; the slowest-saturating cell
    # determines that count, which is `max(added_n)` here.
    size_delta = max(max(added0, added1), max(added2, added3))

    if size_delta > 0 do
      new_size = :atomics.add_get(sketch.size, 1, size_delta)

      if new_size >= sketch.sample_size do
        reset(sketch)
      end
    end

    :ok
  end

  @doc """
  Zeroes every counter and resets the increment count.

  Unlike `reset/1` (which halves counters as part of the aging cycle), this
  function fully clears the sketch. Intended for whole-cache invalidation
  (e.g., `delete_all/1`) where the keys the sketch tracks no longer exist.

  ## Examples

      iex> sketch = Nebulex.TinyLFU.FrequencySketch.new(100)
      iex> Nebulex.TinyLFU.FrequencySketch.increment(sketch, "key")
      iex> Nebulex.TinyLFU.FrequencySketch.clear(sketch)
      :ok
      iex> Nebulex.TinyLFU.FrequencySketch.frequency(sketch, "key")
      0

  """
  @spec clear(t()) :: :ok
  def clear(%__MODULE__{table: table, table_length: table_length, size: size}) do
    :ok = Enum.each(1..table_length, &:atomics.put(table, &1, 0))

    :atomics.put(size, 1, 0)
  end

  @doc """
  Reduces every counter by half of its original value.

  This aging process keeps the sketch fresh by halving all counters when the
  number of recorded events reaches the sample size threshold. The size
  counter is also adjusted to account for the reduction.
  """
  @spec reset(t()) :: :ok
  def reset(%__MODULE__{} = sketch) do
    # Halve all counters and count how many were odd (lost a bit during shift).
    # @reset_mask (0x77...) clears the high bit of each nibble after the shift
    # to prevent bits from bleeding into adjacent counters.
    # @one_mask (0x11...) isolates the lowest bit of each counter to count odd ones.
    count =
      Enum.reduce(1..sketch.table_length, 0, fn i, acc ->
        current = :atomics.get(sketch.table, i)
        :ok = :atomics.put(sketch.table, i, current >>> 1 &&& @reset_mask)

        acc + popcount(current &&& @one_mask)
      end)

    # Adjust the size counter: subtract the rounding error (count / 4 accounts
    # for 4 counters per key) and halve to reflect the halved counters.
    size = :atomics.get(sketch.size, 1)
    new_size = max(0, size - (count >>> 2)) >>> 1

    :atomics.put(sketch.size, 1, new_size)
  end

  ## Private functions

  # Inline common instructions.
  @compile inline: [hash: 1, spread: 1, rehash: 1]

  # Hashes an Erlang term to a 32-bit integer.
  defp hash(key), do: :erlang.phash2(key, 1 <<< 32)

  # Applies a supplemental hash function to defend against poor quality hashes.
  # Ported from Caffeine's spread() — uses Hash Function Prospector's three-round
  # mixing constants (0xED5AD4BB, 0xAC4C1B51) for high avalanche quality.
  defp spread(x) do
    x = bxor(x, x >>> 17)
    x = x * 0xED5AD4BB &&& @int32_mask
    x = bxor(x, x >>> 11)
    x = x * 0xAC4C1B51 &&& @int32_mask

    bxor(x, x >>> 15) &&& @int32_mask
  end

  # Applies another round of hashing (constant 0x31848BAB) to derive
  # counter_hash independently from block_hash.
  defp rehash(x) do
    x = x * 0x31848BAB &&& @int32_mask

    bxor(x, x >>> 14) &&& @int32_mask
  end

  # Reads the 4-bit counter at position `index` (0..15) within the element
  # at 1-based `slot` in the atomics table.
  defp counter_at(table, slot, index) do
    current = :atomics.get(table, slot + 1)

    # Each counter is 4 bits wide; <<< 2 converts counter index to bit offset
    current >>> (index <<< 2) &&& @counter_mask
  end

  # Adds up to `count` to the 4-bit counter at position `index` within
  # the element at 0-based `slot`, capped at the counter's maximum (15).
  # Returns the actual amount added (0..count).
  defp increment_at(table, slot, index, count) do
    # Each counter is 4 bits wide; <<< 2 converts counter index to bit offset
    offset = index <<< 2

    # Mask isolates the 4-bit counter at the given offset
    mask = @counter_mask <<< offset

    # 1-based index for :atomics
    atomics_index = slot + 1
    current = :atomics.get(table, atomics_index)

    # How much room is left in this 4-bit counter before saturation?
    current_count = (current &&& mask) >>> offset
    add = min(count, @counter_mask - current_count)

    if add > 0 do
      :atomics.put(table, atomics_index, current + (add <<< offset))
    end

    add
  end

  # Rounds up to the nearest power of two.
  defp ceiling_power_of_two(n) when n <= 1, do: 1
  defp ceiling_power_of_two(n), do: 1 <<< bit_length(n - 1)

  defp bit_length(n) when n > 0, do: do_bit_length(n, 0)

  defp do_bit_length(0, acc), do: acc
  defp do_bit_length(n, acc), do: do_bit_length(n >>> 1, acc + 1)

  # Population count (number of 1-bits in a 64-bit integer).
  # Uses the standard parallel bit-counting algorithm.
  defp popcount(x) do
    x = x - (x >>> 1 &&& 0x5555555555555555)
    x = (x &&& 0x3333333333333333) + (x >>> 2 &&& 0x3333333333333333)
    x = x + (x >>> 4) &&& 0x0F0F0F0F0F0F0F0F

    (x * 0x0101010101010101) >>> 56
  end
end
