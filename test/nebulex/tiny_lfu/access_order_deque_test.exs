defmodule Nebulex.TinyLFU.AccessOrderDequeTest do
  use ExUnit.Case, async: true

  alias Nebulex.TinyLFU.AccessOrderDeque

  setup context do
    name = :"deque_#{context.test}"
    start_supervised!({AccessOrderDeque, name: name})
    deque = AccessOrderDeque.get_deque(name)

    %{deque: deque}
  end

  describe "start_link/1 and get_deque/1" do
    test "creates a deque with empty tables", %{deque: deque} do
      assert is_reference(deque.data_table)
      assert is_reference(deque.order_table)
      assert AccessOrderDeque.size(deque) == 0
    end
  end

  describe "put/2" do
    test "adds a new key", %{deque: deque} do
      assert AccessOrderDeque.put(deque, :a) == :ok
      assert AccessOrderDeque.size(deque) == 1
      assert AccessOrderDeque.member?(deque, :a)
    end

    test "adding multiple keys preserves insertion order", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      assert AccessOrderDeque.size(deque) == 3
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :a}
    end

    test "touching an existing key moves it to MRU", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      # Touch :a — should move it to MRU (most recent)
      AccessOrderDeque.put(deque, :a)

      # :b is now the LRU
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :b}
      # Size unchanged — no duplicate
      assert AccessOrderDeque.size(deque) == 3
    end

    test "touching does not create duplicates", %{deque: deque} do
      for _ <- 1..10 do
        AccessOrderDeque.put(deque, :a)
      end

      assert AccessOrderDeque.size(deque) == 1
    end

    test "works with various key types", %{deque: deque} do
      keys = ["string", :atom, 42, 3.14, {1, 2}, [1, 2, 3], %{a: 1}]

      for key <- keys do
        AccessOrderDeque.put(deque, key)
      end

      assert AccessOrderDeque.size(deque) == length(keys)

      for key <- keys do
        assert AccessOrderDeque.member?(deque, key)
      end
    end
  end

  describe "remove/2" do
    test "removes an existing key", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      assert AccessOrderDeque.remove(deque, :a) == {:ok, :a}
      assert AccessOrderDeque.size(deque) == 0
      refute AccessOrderDeque.member?(deque, :a)
    end

    test "returns :error for non-existent key", %{deque: deque} do
      assert AccessOrderDeque.remove(deque, :missing) == :error
    end

    test "removing LRU updates the next LRU", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      AccessOrderDeque.remove(deque, :a)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :b}
    end

    test "removing middle entry preserves order of others", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      AccessOrderDeque.remove(deque, :b)
      assert AccessOrderDeque.size(deque) == 2
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :a}

      # Evict :a, then :c should be next
      AccessOrderDeque.evict_lru(deque)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :c}
    end
  end

  describe "evict_lru/1" do
    test "returns :empty for empty deque", %{deque: deque} do
      assert AccessOrderDeque.evict_lru(deque) == :empty
    end

    test "evicts the oldest entry", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      assert AccessOrderDeque.evict_lru(deque) == {:ok, :a}
      assert AccessOrderDeque.size(deque) == 2
      refute AccessOrderDeque.member?(deque, :a)
    end

    test "evicts in LRU order", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      assert AccessOrderDeque.evict_lru(deque) == {:ok, :a}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :b}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :c}
      assert AccessOrderDeque.evict_lru(deque) == :empty
    end

    test "touching changes eviction order", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :c)

      # Touch :a — moves to MRU
      AccessOrderDeque.put(deque, :a)

      # :b is now LRU (oldest untouched)
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :b}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :c}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :a}
    end
  end

  describe "peek_lru/1" do
    test "returns :empty for empty deque", %{deque: deque} do
      assert AccessOrderDeque.peek_lru(deque) == :empty
    end

    test "does not remove the entry", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)

      assert AccessOrderDeque.peek_lru(deque) == {:ok, :a}
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :a}
      assert AccessOrderDeque.size(deque) == 1
    end
  end

  describe "member?/2" do
    test "returns false for non-existent key", %{deque: deque} do
      refute AccessOrderDeque.member?(deque, :missing)
    end

    test "returns true after put", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      assert AccessOrderDeque.member?(deque, :a)
    end

    test "returns false after remove", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.remove(deque, :a)
      refute AccessOrderDeque.member?(deque, :a)
    end

    test "returns false after evict_lru", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.evict_lru(deque)
      refute AccessOrderDeque.member?(deque, :a)
    end
  end

  describe "size/1" do
    test "returns 0 for empty deque", %{deque: deque} do
      assert AccessOrderDeque.size(deque) == 0
    end

    test "increments on put", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      assert AccessOrderDeque.size(deque) == 1

      AccessOrderDeque.put(deque, :b)
      assert AccessOrderDeque.size(deque) == 2
    end

    test "does not increment on touch (existing key)", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :a)
      assert AccessOrderDeque.size(deque) == 1
    end

    test "decrements on remove", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.remove(deque, :a)
      assert AccessOrderDeque.size(deque) == 1
    end

    test "decrements on evict_lru", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.evict_lru(deque)
      assert AccessOrderDeque.size(deque) == 1
    end
  end

  describe "clear/1" do
    test "is a no-op on an empty deque", %{deque: deque} do
      assert AccessOrderDeque.clear(deque) == :ok
      assert AccessOrderDeque.size(deque) == 0
    end

    test "wipes all entries", %{deque: deque} do
      for key <- [:a, :b, :c], do: AccessOrderDeque.put(deque, key)
      assert AccessOrderDeque.size(deque) == 3

      assert AccessOrderDeque.clear(deque) == :ok
      assert AccessOrderDeque.size(deque) == 0
      assert AccessOrderDeque.peek_lru(deque) == :empty
      refute AccessOrderDeque.member?(deque, :a)
      refute AccessOrderDeque.member?(deque, :b)
      refute AccessOrderDeque.member?(deque, :c)
    end

    test "leaves the deque usable for subsequent puts", %{deque: deque} do
      for key <- [:a, :b], do: AccessOrderDeque.put(deque, key)
      :ok = AccessOrderDeque.clear(deque)

      AccessOrderDeque.put(deque, :x)
      AccessOrderDeque.put(deque, :y)

      assert AccessOrderDeque.size(deque) == 2
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :x}
    end
  end

  describe "LRU ordering with mixed operations" do
    test "complex sequence of puts, touches, removes, and evictions", %{deque: deque} do
      # Insert a, b, c, d, e
      for key <- [:a, :b, :c, :d, :e] do
        AccessOrderDeque.put(deque, key)
      end

      # Order: a(LRU) → b → c → d → e(MRU)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :a}

      # Touch :b and :a — move them to MRU
      AccessOrderDeque.put(deque, :b)
      AccessOrderDeque.put(deque, :a)

      # Order: c(LRU) → d → e → b → a(MRU)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :c}

      # Remove :d from the middle
      AccessOrderDeque.remove(deque, :d)

      # Order: c(LRU) → e → b → a(MRU)
      assert AccessOrderDeque.size(deque) == 4

      # Evict LRU (should be :c)
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :c}

      # Order: e(LRU) → b → a(MRU)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :e}

      # Evict all remaining
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :e}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :b}
      assert AccessOrderDeque.evict_lru(deque) == {:ok, :a}
      assert AccessOrderDeque.evict_lru(deque) == :empty
    end

    test "put after remove re-adds at MRU", %{deque: deque} do
      AccessOrderDeque.put(deque, :a)
      AccessOrderDeque.put(deque, :b)

      AccessOrderDeque.remove(deque, :a)
      AccessOrderDeque.put(deque, :a)

      # :b is LRU, :a is MRU (re-added)
      assert AccessOrderDeque.peek_lru(deque) == {:ok, :b}
      assert AccessOrderDeque.size(deque) == 2
    end
  end
end
