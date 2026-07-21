defmodule Nebulex.Adapters.TinyLFUTest do
  use ExUnit.Case, async: true

  # Inherit tests
  use Nebulex.CacheTestCase,
    only: [
      Nebulex.Cache.KVTest,
      Nebulex.Cache.KVExpirationTest,
      Nebulex.Cache.KVPropTest,
      Nebulex.Cache.QueryableTest,
      Nebulex.Cache.QueryableExpirationTest,
      Nebulex.Cache.QueryableQueryErrorTest
      # Nebulex.Cache.TransactionTest,
      # Nebulex.Cache.ObservableTest
    ]

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 3]

  alias Nebulex.Adapters.TinyLFU.TestCache, as: Cache

  # Shared fixture: a key with a NESTED map — the case that breaks ETS
  # match-spec equality on the versioned buffer path, so it's the real
  # exercise of the :key_hasher option.
  @map_key %{tenant: "acme", id: %{region: "us-east", shard: 7}}

  setup_with_dynamic_cache Cache, :tiny_lfu_test, max_size: 1_000

  describe "map values" do
    test "put and fetch with simple map value", %{cache: cache} do
      assert cache.put!(:key1, %{name: "alice", age: 30}) == :ok
      assert cache.fetch!(:key1) == %{name: "alice", age: 30}
    end

    test "put and replace with nested map value", %{cache: cache} do
      value1 = %{
        users: %{admin: %{name: "alice", roles: [:admin, :user]}},
        meta: %{nested: %{deep: %{level: 3}}}
      }

      value2 = %{
        users: %{admin: %{name: "alice", roles: [:admin]}},
        meta: %{nested: %{deep: %{level: 3}}}
      }

      assert cache.put!(:key2, value1) == :ok
      assert cache.fetch!(:key2) == value1

      assert cache.replace!(:key2, value2) == true
      assert cache.fetch!(:key2) == value2
    end

    test "put and fetch with tuple containing maps", %{cache: cache} do
      value = {:ok, %{a: 1, b: %{c: 2}}}

      assert cache.put!(:key3, value) == :ok
      assert cache.fetch!(:key3) == value
    end

    test "replace with map value", %{cache: cache} do
      assert cache.put!(:key4, %{v: 1}) == :ok
      assert cache.replace!(:key4, %{v: 2, extra: %{nested: true}}) == true
      assert cache.fetch!(:key4) == %{v: 2, extra: %{nested: true}}
    end
  end

  describe "map keys (default key_hasher: true)" do
    test "put/fetch/delete and re-access a map-containing key", %{cache: cache} do
      # First write, then a second write on the SAME map key — without hashing
      # this second write would raise; the default hasher makes it work.
      assert cache.put!(@map_key, 1) == :ok
      assert cache.put!(@map_key, 2) == :ok
      assert cache.fetch!(@map_key) == 2

      # Reads and deletes on the map key also round-trip.
      assert cache.fetch!(@map_key) == 2
      assert cache.delete!(@map_key) == :ok
      assert {:error, %Nebulex.KeyError{}} = cache.fetch(@map_key)
    end

    test "map nested inside a tuple key", %{cache: cache} do
      key = {:user, %{id: 1}}

      assert cache.put!(key, "a") == :ok
      assert cache.put!(key, "b") == :ok
      assert cache.fetch!(key) == "b"
    end

    test "batch put_all!/delete_all with map-containing keys", %{cache: cache} do
      k1 = %{tenant: "a", id: %{shard: 1}}
      k2 = %{tenant: "b", id: %{shard: 2}}

      # Bulk path goes through schedule_write_all -> Tidefall.HashMap.put_all_newer,
      # which must thread the same key_hasher as the single-key path.
      assert cache.put_all!(%{k1 => 1, k2 => 2}) == :ok
      assert cache.fetch!(k1) == 1
      assert cache.fetch!(k2) == 2

      assert cache.delete_all!(in: [k1, k2]) == 2
      assert {:error, %Nebulex.KeyError{}} = cache.fetch(k1)
      assert {:error, %Nebulex.KeyError{}} = cache.fetch(k2)
    end
  end

  describe "key_hasher: custom fun (collision-free)" do
    setup do
      cache_name = :tiny_lfu_custom_hasher
      default = Cache.get_dynamic_cache()

      pid =
        {Cache,
         [
           name: cache_name,
           max_size: 100,
           # A collision-free, deterministic hasher for exact key identity in
           # the policy layer (the fun/1 override of the default phash2).
           buffer_opts: [key_hasher: &:erlang.term_to_binary(&1, [:deterministic])]
         ]}
        |> Supervisor.child_spec(id: cache_name)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(cache_name)
      on_exit(fn -> Cache.put_dynamic_cache(default) end)

      %{cache: Cache, pid: pid}
    end

    test "map-containing keys round-trip with a custom hasher", %{cache: cache} do
      assert cache.put!(@map_key, 1) == :ok
      assert cache.put!(@map_key, 2) == :ok
      assert cache.fetch!(@map_key) == 2
      assert cache.delete!(@map_key) == :ok
    end
  end

  describe "key_hasher: false (hashing disabled)" do
    setup do
      cache_name = :tiny_lfu_no_hasher
      default = Cache.get_dynamic_cache()

      pid =
        {Cache,
         [
           name: cache_name,
           max_size: 100,
           # Disable hashing. Use a long interval so both writes of a key land
           # in the same flush window (the map-key raise happens on the 2nd
           # same-window versioned write).
           buffer_opts: [key_hasher: false, processing_interval: 60_000]
         ]}
        |> Supervisor.child_spec(id: cache_name)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(cache_name)
      on_exit(fn -> Cache.put_dynamic_cache(default) end)

      %{cache: Cache, pid: pid}
    end

    test "simple keys work with hashing disabled", %{cache: cache} do
      assert cache.put!(:simple, 1) == :ok
      assert cache.put!(:simple, 2) == :ok
      assert cache.fetch!(:simple) == 2
      assert cache.delete!(:simple) == :ok
    end

    test "a second write on a nested-map key raises when hashing is disabled",
         %{cache: cache} do
      # First write succeeds (insert-new path); the second same-window write
      # hits Tidefall's select_replace, which can't express a map key in the
      # replacement position. This pins the documented `key_hasher: false`
      # limitation so it can't silently change.
      assert cache.put!(@map_key, 1) == :ok
      assert_raise ArgumentError, fn -> cache.put!(@map_key, 2) end
    end
  end

  describe "stats: false start option" do
    setup do
      cache_name = :tiny_lfu_no_stats
      default = Cache.get_dynamic_cache()

      pid =
        {Cache, [name: cache_name, max_size: 100, stats: false]}
        |> Supervisor.child_spec(id: cache_name)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(cache_name)
      on_exit(fn -> Cache.put_dynamic_cache(default) end)

      {:ok, cache: Cache, cache_pid: pid}
    end

    test "skips initialising the stats counter (basic ops still work)", %{cache: cache} do
      assert cache.put!(:a, 1) == :ok
      assert cache.fetch!(:a) == 1
      assert cache.delete!(:a) == :ok
    end
  end

  describe "queryable edge cases (coverage)" do
    test "touch on an expired key returns false and removes it from ETS",
         %{cache: cache} do
      :ok = cache.put!(:expiring, "v", ttl: 10)
      Process.sleep(30)

      assert cache.touch!(:expiring) == false
      assert cache.fetch(:expiring) == {:error, %Nebulex.KeyError{key: :expiring}}
    end

    test "delete_all with a custom match spec deletes only matching entries",
         %{cache: cache} do
      for i <- 1..10, do: cache.put!({:m, i}, i)

      # Match all entries whose key tuple's second element is even.
      ms = [
        {{:_, {:"$1", :"$2"}, :"$3", :"$4"}, [{:==, {:rem, :"$2", 2}, 0}], [true]}
      ]

      {:ok, count} = cache.delete_all(query: ms)
      assert count == 5
      assert cache.count_all!() == 5
    end

    test "stream with a custom match spec paginates", %{cache: cache} do
      for i <- 1..50, do: cache.put!({:s, i}, i)

      ms = [{{:_, :"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}]

      result =
        [query: ms]
        |> cache.stream!(max_entries: 10)
        |> Enum.into(%{})

      for i <- 1..50 do
        assert Map.fetch!(result, {:s, i}) == i
      end
    end

    test "get_all with select: :entry returns {key, value} pairs", %{cache: cache} do
      :ok = cache.put!(:e1, 1)
      :ok = cache.put!(:e2, 2)

      {:ok, entries} = cache.get_all(in: [:e1, :e2], select: :entry)

      assert Enum.sort(entries) == [{:e1, 1}, {:e2, 2}]
    end

    test "stream with select: :entry walks the table via stream_next/3",
         %{cache: cache} do
      :ok = cache.put!(:s1, 1)
      :ok = cache.put!(:s2, 2)

      result = [select: :entry] |> cache.stream!() |> Enum.into(%{})

      assert result == %{s1: 1, s2: 2}
    end

    test "stream {:in, [single_key]} yields only that entry", %{cache: cache} do
      :ok = cache.put!(:only, "v")
      :ok = cache.put!(:other, "x")

      result =
        [in: [:only], select: {:key, :value}]
        |> cache.stream!()
        |> Enum.into(%{})

      assert result == %{only: "v"}
    end

    test "get_all with > 10 keys exercises the chunk-flush recursion",
         %{cache: cache} do
      for i <- 1..15, do: cache.put!("ck_#{i}", i)
      keys = Enum.map(1..15, &"ck_#{&1}")

      {:ok, result} = cache.get_all(in: keys, select: {:key, :value})

      assert length(result) == 15
      assert Enum.sort(result) == Enum.sort(Enum.map(1..15, &{"ck_#{&1}", &1}))
    end

    test "tuple keys round-trip through ets_select_keys + tuple_to_match_spec",
         %{cache: cache} do
      :ok = cache.put!({:user, 1, :profile}, "alice")
      :ok = cache.put!({:user, 2, :profile}, "bob")

      {:ok, result} =
        cache.get_all(in: [{:user, 1, :profile}, {:user, 2, :profile}], select: :value)

      assert Enum.sort(result) == ["alice", "bob"]
    end
  end

  describe "unbounded mode — extended operations" do
    setup do
      cache_name = :unbounded_extended
      default = Cache.get_dynamic_cache()

      pid =
        {Cache, [name: cache_name]}
        |> Supervisor.child_spec(id: cache_name)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(cache_name)
      on_exit(fn -> Cache.put_dynamic_cache(default) end)

      {:ok, cache: Cache, cache_pid: pid}
    end

    test "put_all is a no-op in the buffer pipeline", %{cache: cache} do
      :ok = cache.put_all!(%{a: 1, b: 2, c: 3})
      assert cache.fetch!(:a) == 1
      assert cache.fetch!(:b) == 2
      assert cache.fetch!(:c) == 3
    end

    test "delete is a no-op in the buffer pipeline", %{cache: cache} do
      :ok = cache.put!(:k, "v")
      :ok = cache.delete!(:k)
      assert cache.fetch(:k) == {:error, %Nebulex.KeyError{key: :k}}
    end

    test "delete_all with keys is a no-op in the buffer pipeline", %{cache: cache} do
      :ok = cache.put_all!(%{a: 1, b: 2, c: 3})
      {:ok, 2} = cache.delete_all(in: [:a, :b])
      assert cache.fetch(:a) == {:error, %Nebulex.KeyError{key: :a}}
      assert cache.fetch!(:c) == 3
    end

    test "delete_all clears every entry without a flush event", %{cache: cache} do
      :ok = cache.put_all!(%{a: 1, b: 2})
      {:ok, 2} = cache.delete_all()
      assert cache.count_all!() == 0
    end
  end

  describe "delete_all clears policy state" do
    alias Nebulex.TinyLFU.{AccessOrderDeque, Maintenance}
    alias Nebulex.TinyLFU.Supervisor, as: TinyLFUSupervisor

    setup do
      cache_name = :tiny_lfu_delete_all_test
      default_dynamic_cache = Cache.get_dynamic_cache()

      pid =
        {
          Cache,
          # Fast buffer flush so the test doesn't have to wait full seconds
          # for the maintenance pipeline to drain.
          name: cache_name,
          max_size: 100,
          buffer_opts: [processing_interval: 5, processing_batch_size: 200]
        }
        |> Supervisor.child_spec(id: cache_name)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(cache_name)

      on_exit(fn -> Cache.put_dynamic_cache(default_dynamic_cache) end)

      deques =
        for segment <- TinyLFUSupervisor.deque_segments(), into: %{} do
          deque =
            cache_name
            |> TinyLFUSupervisor.deque_name(segment)
            |> AccessOrderDeque.get_deque()

          {segment, deque}
        end

      {:ok, cache_name: cache_name, cache_pid: pid, deques: deques}
    end

    test "wipes the maintenance deques after delete_all!", ctx do
      for i <- 1..50, do: Cache.put!("k#{i}", i)

      # Buffer must drain at least once for the deques to populate.
      assert eventually(fn -> total_deque_size(ctx.deques) > 0 end)

      Cache.delete_all!()

      # After the flush event drains, every deque should be empty.
      assert eventually(fn -> total_deque_size(ctx.deques) == 0 end)
    end

    test "uses the documented flush sentinel key", _ctx do
      # Locks down the cross-module contract between the adapter
      # (`schedule_flush`) and the maintenance worker.
      assert Maintenance.flush_key() == :__nbx_tinylfu_flush__
    end

    defp total_deque_size(deques) do
      Enum.reduce(deques, 0, fn {_segment, deque}, acc ->
        acc + AccessOrderDeque.size(deque)
      end)
    end

    # Polls until `fun` returns truthy or the timeout (1s) elapses.
    # Returns the truthy value, or fails the test on timeout.
    defp eventually(fun, deadline \\ System.monotonic_time(:millisecond) + 1_000) do
      case fun.() do
        result when result not in [false, nil] ->
          result

        _ ->
          if System.monotonic_time(:millisecond) >= deadline do
            flunk("eventually/1 timed out")
          else
            Process.sleep(10)
            eventually(fun, deadline)
          end
      end
    end
  end

  describe "unbounded cache (no max_size)" do
    setup do
      default_dynamic_cache = Cache.get_dynamic_cache()

      pid =
        {Cache, [name: :unbounded_test]}
        |> Supervisor.child_spec(id: :unbounded_test)
        |> start_supervised!()

      _ = Cache.put_dynamic_cache(:unbounded_test)

      on_exit(fn ->
        Cache.put_dynamic_cache(default_dynamic_cache)
      end)

      {:ok, cache: Cache, cache_pid: pid}
    end

    test "put and fetch without max_size", %{cache: cache} do
      assert cache.put!("a", 1) == :ok
      assert cache.fetch!("a") == 1
    end

    test "entries are not evicted in unbounded mode", %{cache: cache} do
      for i <- 1..100 do
        cache.put!(i, i)
      end

      # Wait for maintenance to process
      Process.sleep(50)

      # All entries should still be present (no eviction)
      assert cache.count_all!() == 100
    end
  end
end
