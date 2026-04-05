defmodule Nebulex.TinyLFU.MaintenanceTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Nebulex.TinyLFU.{AccessOrderDeque, FrequencySketch, Maintenance}

  setup do
    sketch = FrequencySketch.new(1_000)

    [window_deque, probation_deque, protected_deque] =
      for segment <- [:window, :probation, :protected] do
        name = Module.concat([__MODULE__, segment])

        start_supervised!({AccessOrderDeque, name: name}, id: name)

        AccessOrderDeque.get_deque(name)
      end

    %{
      sketch: sketch,
      window: window_deque,
      probation: probation_deque,
      protected: protected_deque
    }
  end

  ## Write Events

  describe "write events — basic" do
    test "adds key to window deque", ctx do
      process([write_event(:a)], ctx)

      assert AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.size(ctx.window) == 1
    end

    test "increments frequency sketch on write", ctx do
      process([write_event(:a)], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :a) >= 1
    end

    test "multiple writes to same key don't duplicate in window", ctx do
      process([write_event(:a), write_event(:a), write_event(:a)], ctx)

      assert AccessOrderDeque.size(ctx.window) == 1
      assert AccessOrderDeque.member?(ctx.window, :a)
    end

    test "writing existing key touches it (moves to MRU) in window", ctx do
      process([write_event(:a), write_event(:b)], ctx)
      assert AccessOrderDeque.peek_lru(ctx.window) == {:ok, :a}

      # Re-write :a — should move to MRU
      process([write_event(:a)], ctx)
      assert AccessOrderDeque.peek_lru(ctx.window) == {:ok, :b}
    end

    test "window does not evict when at capacity", ctx do
      process([write_event(:a), write_event(:b)], ctx, window_max: 2)

      assert AccessOrderDeque.size(ctx.window) == 2
      assert AccessOrderDeque.size(ctx.probation) == 0
    end
  end

  describe "write events — window eviction" do
    test "evicts LRU from window when exceeding capacity", ctx do
      # window_max=2, writing 3 keys evicts :a
      process(
        [write_event(:a), write_event(:b), write_event(:c)],
        ctx,
        window_max: 2
      )

      refute AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.size(ctx.window) == 2
    end

    test "window victim admitted to empty probation directly", ctx do
      process(
        [write_event(:a), write_event(:b)],
        ctx,
        window_max: 1
      )

      # :a evicted from window, probation was empty → admitted directly
      assert AccessOrderDeque.member?(ctx.probation, :a)
      assert AccessOrderDeque.member?(ctx.window, :b)
    end

    test "sequential evictions fill probation or discard via admission", ctx do
      # window_max=1: each write evicts the previous key
      process(
        [write_event(:a), write_event(:b), write_event(:c), write_event(:d)],
        ctx,
        window_max: 1
      )

      # Only :d should be in window
      assert AccessOrderDeque.member?(ctx.window, :d)
      assert AccessOrderDeque.size(ctx.window) == 1

      # Evicted keys either went to probation or were discarded by admission.
      # Total across all deques should be <= 4 (some may have been discarded)
      assert total_size(ctx) <= 4
      assert total_size(ctx) >= 1
    end
  end

  describe "write events — admission policy" do
    test "admits window victim with higher frequency than main victim", ctx do
      # Give :high_freq a high frequency
      boost_frequency(ctx.sketch, :high_freq)

      # Put :low_freq in probation (will be the victim)
      AccessOrderDeque.put(ctx.probation, :low_freq)

      # Write :high_freq (enters window), then :pusher evicts :high_freq
      process(
        [write_event(:high_freq), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      # :high_freq admitted to probation, :low_freq evicted
      assert AccessOrderDeque.member?(ctx.probation, :high_freq)
      refute AccessOrderDeque.member?(ctx.probation, :low_freq)
    end

    test "rejects window victim with lower frequency than main victim", ctx do
      # Give :popular a high frequency
      boost_frequency(ctx.sketch, :popular)

      # Put :popular in probation
      AccessOrderDeque.put(ctx.probation, :popular)

      # Write :unpopular (enters window), then :pusher evicts :unpopular
      process(
        [write_event(:unpopular), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      # :popular retained, :unpopular discarded
      assert AccessOrderDeque.member?(ctx.probation, :popular)
      refute AccessOrderDeque.member?(ctx.probation, :unpopular)
      refute AccessOrderDeque.member?(ctx.window, :unpopular)
    end

    test "on equal frequency, victim is retained (candidate rejected)", ctx do
      # Both keys have frequency 0 initially; write increments each by 1
      # After write_event(:candidate), freq(:candidate) = 1
      # freq(:victim) = 0 (never written through process)
      # But we need them equal — so increment :victim once manually
      FrequencySketch.increment(ctx.sketch, :victim)

      AccessOrderDeque.put(ctx.probation, :victim)

      # Write :candidate, then :pusher evicts :candidate
      # freq(:candidate) = 1 (from the write), freq(:victim) = 1
      # Equal frequency → victim retained (candidate needs strictly greater)
      process(
        [write_event(:candidate), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      assert AccessOrderDeque.member?(ctx.probation, :victim)
      refute AccessOrderDeque.member?(ctx.probation, :candidate)
    end

    test "admission compares against probation LRU specifically", ctx do
      # Probation has [:old, :newer] — :old is LRU
      AccessOrderDeque.put(ctx.probation, :old)
      AccessOrderDeque.put(ctx.probation, :newer)

      # Give :candidate higher freq than :old
      boost_frequency(ctx.sketch, :candidate)

      process(
        [write_event(:candidate), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      # :candidate admitted, :old (LRU) evicted
      assert AccessOrderDeque.member?(ctx.probation, :candidate)
      refute AccessOrderDeque.member?(ctx.probation, :old)
      # :newer should still be there
      assert AccessOrderDeque.member?(ctx.probation, :newer)
    end
  end

  ## Read Events

  describe "read events — window" do
    test "touches key in window (moves to MRU)", ctx do
      AccessOrderDeque.put(ctx.window, :a)
      AccessOrderDeque.put(ctx.window, :b)
      AccessOrderDeque.put(ctx.window, :c)

      process([read_event(:a)], ctx)

      # :a moved to MRU, :b is now LRU
      assert AccessOrderDeque.peek_lru(ctx.window) == {:ok, :b}
      assert AccessOrderDeque.size(ctx.window) == 3
    end

    test "multiple reads of same key in window", ctx do
      AccessOrderDeque.put(ctx.window, :a)
      AccessOrderDeque.put(ctx.window, :b)

      process([read_event(:a), read_event(:a), read_event(:a)], ctx)

      assert AccessOrderDeque.size(ctx.window) == 2
      assert AccessOrderDeque.peek_lru(ctx.window) == {:ok, :b}
    end
  end

  describe "read events — probation to protected promotion" do
    test "promotes key from probation to protected", ctx do
      AccessOrderDeque.put(ctx.probation, :a)

      process([read_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.probation, :a)
      assert AccessOrderDeque.member?(ctx.protected, :a)
    end

    test "promotion places key at MRU of protected", ctx do
      AccessOrderDeque.put(ctx.protected, :existing)
      AccessOrderDeque.put(ctx.probation, :promoted)

      process([read_event(:promoted)], ctx)

      # :existing is LRU, :promoted is MRU
      assert AccessOrderDeque.peek_lru(ctx.protected) == {:ok, :existing}
    end

    test "demotes LRU from protected to probation when full", ctx do
      # Fill protected to max (3)
      AccessOrderDeque.put(ctx.protected, :x)
      AccessOrderDeque.put(ctx.protected, :y)
      AccessOrderDeque.put(ctx.protected, :z)

      # Promote :a from probation → protected exceeds max → :x demoted
      AccessOrderDeque.put(ctx.probation, :a)
      process([read_event(:a)], ctx, protected_max: 3)

      assert AccessOrderDeque.member?(ctx.protected, :a)
      assert AccessOrderDeque.member?(ctx.probation, :x)
      refute AccessOrderDeque.member?(ctx.protected, :x)
    end

    test "multiple promotions cause chain of demotions", ctx do
      # Protected full: [:x, :y, :z]
      AccessOrderDeque.put(ctx.protected, :x)
      AccessOrderDeque.put(ctx.protected, :y)
      AccessOrderDeque.put(ctx.protected, :z)

      # Promote :a and :b from probation
      AccessOrderDeque.put(ctx.probation, :a)
      AccessOrderDeque.put(ctx.probation, :b)

      process([read_event(:a), read_event(:b)], ctx, protected_max: 3)

      # :a and :b promoted, :x and :y demoted
      assert AccessOrderDeque.member?(ctx.protected, :a)
      assert AccessOrderDeque.member?(ctx.protected, :b)
      assert AccessOrderDeque.member?(ctx.protected, :z)
      assert AccessOrderDeque.member?(ctx.probation, :x)
      assert AccessOrderDeque.member?(ctx.probation, :y)
    end

    test "demoted entry goes to probation MRU", ctx do
      AccessOrderDeque.put(ctx.protected, :x)
      AccessOrderDeque.put(ctx.protected, :y)
      AccessOrderDeque.put(ctx.protected, :z)

      # Add existing probation entries
      AccessOrderDeque.put(ctx.probation, :old)

      # Promote :a → :x demoted to probation
      AccessOrderDeque.put(ctx.probation, :a)
      process([read_event(:a)], ctx, protected_max: 3)

      # :old should be LRU in probation (was there before :x was demoted)
      assert AccessOrderDeque.peek_lru(ctx.probation) == {:ok, :old}
    end
  end

  describe "read events — protected" do
    test "touches key in protected (moves to MRU)", ctx do
      AccessOrderDeque.put(ctx.protected, :a)
      AccessOrderDeque.put(ctx.protected, :b)

      process([read_event(:a)], ctx)

      assert AccessOrderDeque.peek_lru(ctx.protected) == {:ok, :b}
    end

    test "touch in protected does not change size", ctx do
      AccessOrderDeque.put(ctx.protected, :a)
      AccessOrderDeque.put(ctx.protected, :b)

      process([read_event(:a)], ctx)

      assert AccessOrderDeque.size(ctx.protected) == 2
    end
  end

  describe "read events — key not found (stale access guard)" do
    test "skips key not in any deque", ctx do
      process([read_event(:ghost)], ctx)

      assert total_size(ctx) == 0
    end

    test "does not crash on read for non-existent key with populated deques", ctx do
      AccessOrderDeque.put(ctx.window, :a)
      AccessOrderDeque.put(ctx.probation, :b)
      AccessOrderDeque.put(ctx.protected, :c)

      process([read_event(:missing)], ctx)

      # All existing keys unaffected
      assert AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.member?(ctx.probation, :b)
      assert AccessOrderDeque.member?(ctx.protected, :c)
    end

    test "stale read after delete is a no-op", ctx do
      # Simulates: read buffered for :a, then :a deleted before read is processed
      process([write_event(:a)], ctx)
      assert locate_key(ctx, :a) == :window

      # Delete then stale read in the same batch
      process([delete_event(:a), read_event(:a)], ctx)

      assert locate_key(ctx, :a) == :none
      assert total_size(ctx) == 0
    end

    test "stale read after eviction is a no-op", ctx do
      # :a gets evicted from window and discarded by admission,
      # then a stale read arrives
      boost_frequency(ctx.sketch, :victim_in_probation)
      AccessOrderDeque.put(ctx.probation, :victim_in_probation)

      # Write :a then :b (evicts :a), :a loses admission → discarded
      process([write_event(:a), write_event(:b)], ctx, window_max: 1)

      if locate_key(ctx, :a) == :none do
        # Stale read for :a — should be a no-op
        size_before = total_size(ctx)
        process([read_event(:a)], ctx)

        assert locate_key(ctx, :a) == :none
        assert total_size(ctx) == size_before
      end
    end
  end

  ## Delete Events

  describe "delete events" do
    test "removes key from window", ctx do
      AccessOrderDeque.put(ctx.window, :a)
      process([delete_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.size(ctx.window) == 0
    end

    test "removes key from probation", ctx do
      AccessOrderDeque.put(ctx.probation, :a)
      process([delete_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.probation, :a)
    end

    test "removes key from protected", ctx do
      AccessOrderDeque.put(ctx.protected, :a)
      process([delete_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.protected, :a)
    end

    test "no-op for key not in any deque", ctx do
      process([delete_event(:missing)], ctx)

      assert total_size(ctx) == 0
    end

    test "delete after write removes the key", ctx do
      process([write_event(:a), delete_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.window, :a)
      assert total_size(ctx) == 0
    end

    test "delete key from probation does not affect other entries", ctx do
      AccessOrderDeque.put(ctx.probation, :a)
      AccessOrderDeque.put(ctx.probation, :b)
      AccessOrderDeque.put(ctx.probation, :c)

      process([delete_event(:b)], ctx)

      assert AccessOrderDeque.member?(ctx.probation, :a)
      refute AccessOrderDeque.member?(ctx.probation, :b)
      assert AccessOrderDeque.member?(ctx.probation, :c)
      assert AccessOrderDeque.size(ctx.probation) == 2
    end
  end

  ## Coalesced events (buffer dedup → updates count)

  describe "coalesced events" do
    test "write event with updates=N ticks the sketch N+1 times", ctx do
      # The buffer coalesces repeated puts on the same key into a single
      # event with `updates` set to the number of additional puts. The
      # maintenance worker must replay them so a hot key isn't undercounted.
      process([{:hot, {:write, :v}, 7}], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :hot) == 8
    end

    test "read event with updates=N ticks the sketch N+1 times", ctx do
      AccessOrderDeque.put(ctx.window, :hot)

      process([{:hot, :read, 4}], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :hot) == 5
    end

    test "increment count is capped at the sketch saturation point (15)", ctx do
      # 1000 coalesced events on a fresh key cannot push the 4-bit counter
      # above 15, so we cap to avoid wasted increment calls.
      process([{:flooded, {:write, :v}, 999}], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :flooded) == 15
    end

    test "single-event (updates=0) still ticks the sketch once", ctx do
      process([write_event(:cold)], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :cold) == 1
    end
  end

  ## Flush Events (whole-cache invalidation)

  describe "flush events" do
    test "wipes all three deques", ctx do
      AccessOrderDeque.put(ctx.window, :w)
      AccessOrderDeque.put(ctx.probation, :pb)
      AccessOrderDeque.put(ctx.protected, :pt)

      process([flush_event()], ctx)

      assert AccessOrderDeque.size(ctx.window) == 0
      assert AccessOrderDeque.size(ctx.probation) == 0
      assert AccessOrderDeque.size(ctx.protected) == 0
    end

    test "zeroes the frequency sketch", ctx do
      boost_frequency(ctx.sketch, :hot, 12)
      assert FrequencySketch.frequency(ctx.sketch, :hot) == 12

      process([flush_event()], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :hot) == 0
    end

    test "is processed before other events in the same batch", ctx do
      # A write batched alongside a flush must end up in a freshly
      # cleared deque, not be re-added on top of the wipe.
      AccessOrderDeque.put(ctx.window, :stale)

      process([write_event(:fresh), flush_event()], ctx)

      refute AccessOrderDeque.member?(ctx.window, :stale)
      assert AccessOrderDeque.member?(ctx.window, :fresh)
      assert AccessOrderDeque.size(ctx.window) == 1
    end

    test "drops stale write events for keys not in ETS", ctx do
      data_tab = :ets.new(:flush_stale, [:set, :public])

      # Simulate the bug case: a write was buffered, then `delete_all` ran
      # on the hot path and emptied ETS. The buffer still holds the write
      # event for :stale_write. The maintenance worker must not resurrect
      # it in the deque.
      :ets.insert(data_tab, {:fresh, "fresh_value"})

      process(
        [write_event(:stale_write), flush_event(), write_event(:fresh)],
        ctx,
        data_tab: data_tab
      )

      refute AccessOrderDeque.member?(ctx.window, :stale_write)
      assert AccessOrderDeque.member?(ctx.window, :fresh)
    end
  end

  ## Mixed Event Sequences

  describe "mixed event sequences" do
    test "full lifecycle: write → read (promote) → read (touch) → delete", ctx do
      # Write :a — enters window
      process([write_event(:a)], ctx)
      assert locate_key(ctx, :a) == :window

      # Evict :a from window to probation
      process([write_event(:b), write_event(:c)], ctx, window_max: 1)

      # :a should now be in probation (evicted through admission)
      # (:b pushed :a out, :c pushed :b out — both admitted to empty/growing probation)
      if locate_key(ctx, :a) == :probation do
        # Read :a — promotes to protected
        process([read_event(:a)], ctx)
        assert locate_key(ctx, :a) == :protected

        # Read :a again — touches in protected
        process([read_event(:a)], ctx)
        assert locate_key(ctx, :a) == :protected

        # Delete :a
        process([delete_event(:a)], ctx)
        assert locate_key(ctx, :a) == :none
      end
    end

    test "re-admission: evicted key re-written and re-admitted", ctx do
      # Write :a, evict it
      process([write_event(:a), write_event(:b)], ctx, window_max: 1)
      assert locate_key(ctx, :a) == :probation

      # Delete :a from probation
      process([delete_event(:a)], ctx)
      assert locate_key(ctx, :a) == :none

      # Re-write :a — should enter window again
      process([write_event(:a)], ctx)
      assert locate_key(ctx, :a) == :window
    end

    test "batch processing handles interleaved event types", ctx do
      process(
        [
          write_event(:a),
          write_event(:b),
          write_event(:c),
          read_event(:a),
          delete_event(:b)
        ],
        ctx,
        window_max: 3
      )

      assert AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.member?(ctx.window, :c)
      refute AccessOrderDeque.member?(ctx.window, :b)
      assert AccessOrderDeque.size(ctx.window) == 2
    end

    test "write to key in probation promotes to protected (no duplicate in window)", ctx do
      # Put :a in probation
      AccessOrderDeque.put(ctx.probation, :a)

      # Write :a — triggers on_access which promotes probation → protected
      # Matches Caffeine's UpdateTask → onAccess behavior
      process([write_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.window, :a)
      refute AccessOrderDeque.member?(ctx.probation, :a)
      assert AccessOrderDeque.member?(ctx.protected, :a)
    end

    test "write to key already in protected touches in place", ctx do
      AccessOrderDeque.put(ctx.protected, :a)
      AccessOrderDeque.put(ctx.protected, :b)
      assert AccessOrderDeque.peek_lru(ctx.protected) == {:ok, :a}

      # Write :a — should touch in protected (move to MRU), NOT add to window
      process([write_event(:a)], ctx)

      refute AccessOrderDeque.member?(ctx.window, :a)
      assert AccessOrderDeque.member?(ctx.protected, :a)
      assert AccessOrderDeque.peek_lru(ctx.protected) == {:ok, :b}
    end

    test "stress: many writes with small window", ctx do
      events =
        for i <- 1..20, do: write_event("key_#{i}")

      process(events, ctx, window_max: 3)

      # Window should have at most 3 entries
      assert AccessOrderDeque.size(ctx.window) <= 3

      # All keys should be accounted for somewhere or discarded by admission
      assert total_size(ctx) <= 20
      assert total_size(ctx) > 0
    end
  end

  ## Frequency Sketch Interaction

  describe "frequency sketch interaction" do
    test "write events increment frequency", ctx do
      process([write_event(:a), write_event(:b)], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :a) >= 1
      assert FrequencySketch.frequency(ctx.sketch, :b) >= 1
    end

    test "read events increment frequency (matches Caffeine's onAccess)", ctx do
      AccessOrderDeque.put(ctx.window, :a)

      freq_before = FrequencySketch.frequency(ctx.sketch, :a)
      process([read_event(:a)], ctx)
      freq_after = FrequencySketch.frequency(ctx.sketch, :a)

      assert freq_after > freq_before
    end

    test "multiple reads accumulate frequency", ctx do
      AccessOrderDeque.put(ctx.window, :a)

      process([read_event(:a), read_event(:a), read_event(:a)], ctx)

      assert FrequencySketch.frequency(ctx.sketch, :a) >= 3
    end

    test "admission uses current frequency at decision time", ctx do
      # :candidate has been boosted before it's written
      boost_frequency(ctx.sketch, :candidate, 5)
      AccessOrderDeque.put(ctx.probation, :victim)

      process(
        [write_event(:candidate), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      # :candidate's frequency (5 + 1 from write = 6) > :victim's (0)
      assert AccessOrderDeque.member?(ctx.probation, :candidate)
      refute AccessOrderDeque.member?(ctx.probation, :victim)
    end

    test "frequency capped at 15 does not cause issues", ctx do
      # Max out frequency
      boost_frequency(ctx.sketch, :hot, 20)
      assert FrequencySketch.frequency(ctx.sketch, :hot) == 15

      # Should still work normally in admission
      AccessOrderDeque.put(ctx.probation, :cold)

      process(
        [write_event(:hot), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      assert AccessOrderDeque.member?(ctx.probation, :hot)
    end
  end

  ## ETS Integration

  describe "ETS eviction" do
    setup do
      data_tab = :ets.new(:test_data, [:set, :public])

      %{data_tab: data_tab}
    end

    test "admission eviction deletes loser from ETS", ctx do
      # Put :popular in probation with high frequency
      boost_frequency(ctx.sketch, :popular)
      AccessOrderDeque.put(ctx.probation, :popular)
      :ets.insert(ctx.data_tab, {:popular, "popular_value"})

      # Put :unpopular in ETS (will enter window, then lose admission)
      :ets.insert(ctx.data_tab, {:unpopular, "unpopular_value"})

      # :pusher must be in ETS for its write event to be processed —
      # mirrors the real hot path (cache.put inserts before scheduling).
      :ets.insert(ctx.data_tab, {:pusher, "pusher_value"})

      process(
        [write_event(:unpopular), write_event(:pusher)],
        ctx,
        window_max: 1,
        data_tab: ctx.data_tab
      )

      # :unpopular lost admission → deleted from ETS
      assert :ets.lookup(ctx.data_tab, :unpopular) == []
      # :popular retained
      assert :ets.lookup(ctx.data_tab, :popular) == [{:popular, "popular_value"}]
    end

    test "admission winner causes loser to be deleted from ETS", ctx do
      # Put :low_freq in probation (will be evicted)
      AccessOrderDeque.put(ctx.probation, :low_freq)
      :ets.insert(ctx.data_tab, {:low_freq, "low_value"})

      # Boost :high_freq so it wins admission
      boost_frequency(ctx.sketch, :high_freq)
      :ets.insert(ctx.data_tab, {:high_freq, "high_value"})

      # :pusher must be in ETS for its write event to be processed —
      # mirrors the real hot path (cache.put inserts before scheduling).
      :ets.insert(ctx.data_tab, {:pusher, "pusher_value"})

      process(
        [write_event(:high_freq), write_event(:pusher)],
        ctx,
        window_max: 1,
        data_tab: ctx.data_tab
      )

      # :low_freq lost → deleted from ETS
      assert :ets.lookup(ctx.data_tab, :low_freq) == []
      # :high_freq admitted to probation, still in ETS
      assert :ets.lookup(ctx.data_tab, :high_freq) == [{:high_freq, "high_value"}]
    end

    test "total-size enforcement evicts entries from ETS", ctx do
      # Insert 10 keys into deques and ETS
      for i <- 1..10 do
        key = "key_#{i}"
        AccessOrderDeque.put(ctx.probation, key)
        :ets.insert(ctx.data_tab, {key, "value_#{i}"})
      end

      assert AccessOrderDeque.size(ctx.probation) == 10

      # Process an empty batch with max_size=5 — should evict 5 entries
      process([], ctx, data_tab: ctx.data_tab, max_size: 5)

      assert total_size(ctx) == 5
      assert :ets.info(ctx.data_tab, :size) == 5
    end

    test "total-size enforcement evicts from probation first, then protected", ctx do
      # Put 3 in probation and 3 in protected
      for i <- 1..3 do
        AccessOrderDeque.put(ctx.probation, "prob_#{i}")
        :ets.insert(ctx.data_tab, {"prob_#{i}", i})
      end

      for i <- 1..3 do
        AccessOrderDeque.put(ctx.protected, "prot_#{i}")
        :ets.insert(ctx.data_tab, {"prot_#{i}", i})
      end

      # Enforce max_size=4 — should evict 2 from probation first
      process([], ctx, data_tab: ctx.data_tab, max_size: 4)

      assert total_size(ctx) == 4
      # Probation had 3, should have lost 2 (LRU order: prob_1, prob_2)
      assert AccessOrderDeque.size(ctx.probation) == 1
      assert AccessOrderDeque.size(ctx.protected) == 3
    end

    test "evicts from protected when probation is empty", ctx do
      # Empty probation forces evict_entries to fall through to protected.
      for i <- 1..3 do
        AccessOrderDeque.put(ctx.protected, "prot_#{i}")
        :ets.insert(ctx.data_tab, {"prot_#{i}", i})
      end

      process([], ctx, data_tab: ctx.data_tab, max_size: 1)

      assert AccessOrderDeque.size(ctx.protected) == 1
      # Protected LRU was prot_1; should be gone from ETS.
      assert :ets.lookup(ctx.data_tab, "prot_1") == []
      assert :ets.lookup(ctx.data_tab, "prot_3") == [{"prot_3", 3}]
    end

    test "evicts from window when probation and protected are both empty", ctx do
      # Only window has entries — forces evict_entries to walk all the way
      # down through probation → protected → window.
      for i <- 1..3 do
        AccessOrderDeque.put(ctx.window, "win_#{i}")
        :ets.insert(ctx.data_tab, {"win_#{i}", i})
      end

      process([], ctx, data_tab: ctx.data_tab, max_size: 1)

      assert AccessOrderDeque.size(ctx.window) == 1
      # Window LRU was win_1; should be gone from ETS.
      assert :ets.lookup(ctx.data_tab, "win_1") == []
      assert :ets.lookup(ctx.data_tab, "win_3") == [{"win_3", 3}]
    end

    test "no ETS deletion when data_tab is nil", ctx do
      # This is the default behavior (unit tests without ETS)
      boost_frequency(ctx.sketch, :popular)
      AccessOrderDeque.put(ctx.probation, :popular)

      # Eviction happens but no crash since data_tab is nil
      process(
        [write_event(:unpopular), write_event(:pusher)],
        ctx,
        window_max: 1
      )

      # Just verify it doesn't crash
      assert AccessOrderDeque.member?(ctx.probation, :popular)
    end
  end

  ## Property-Based Tests

  describe "property-based invariants" do
    property "total deque size never exceeds number of unique keys written", ctx do
      check all(
              keys <- list_of(integer(1..50), min_length: 1, max_length: 30),
              window_max <- integer(1..5)
            ) do
        # Reset deques for each property check
        clear_deques(ctx)

        events = Enum.map(keys, &write_event/1)
        process(events, ctx, window_max: window_max)

        unique_count = keys |> Enum.uniq() |> length()

        assert total_size(ctx) <= unique_count
      end
    end

    property "window size never exceeds window_max after processing", ctx do
      check all(
              keys <- list_of(integer(1..100), min_length: 1, max_length: 50),
              window_max <- integer(1..10)
            ) do
        clear_deques(ctx)

        events = Enum.map(keys, &write_event/1)
        process(events, ctx, window_max: window_max)

        assert AccessOrderDeque.size(ctx.window) <= window_max
      end
    end

    property "protected size never exceeds protected_max after promotions", ctx do
      check all(
              num_entries <- integer(1..20),
              protected_max <- integer(1..10)
            ) do
        clear_deques(ctx)

        # Put entries in probation, then read them to promote
        keys = for i <- 1..num_entries, do: "prop_key_#{i}"

        for key <- keys do
          AccessOrderDeque.put(ctx.probation, key)
        end

        events = Enum.map(keys, &read_event/1)
        process(events, ctx, protected_max: protected_max)

        assert AccessOrderDeque.size(ctx.protected) <= protected_max
      end
    end

    property "delete always removes key from all deques", ctx do
      check all(key <- integer(1..1000)) do
        clear_deques(ctx)

        # Write and then delete
        process([write_event(key), delete_event(key)], ctx)

        assert locate_key(ctx, key) == :none
      end
    end

    property "frequency never exceeds 15" do
      sketch = FrequencySketch.new(100)

      check all(
              key <- integer(1..50),
              increments <- integer(1..30)
            ) do
        for _ <- 1..increments do
          FrequencySketch.increment(sketch, key)
        end

        assert FrequencySketch.frequency(sketch, key) <= 15
      end
    end
  end

  ## Helpers

  defp process(events, ctx, opts \\ []) do
    maint_ctx = %Maintenance{
      sketch: ctx.sketch,
      window: ctx.window,
      probation: ctx.probation,
      protected: ctx.protected,
      window_max: Keyword.get(opts, :window_max, 2),
      protected_max: Keyword.get(opts, :protected_max, 3),
      data_tab: Keyword.get(opts, :data_tab),
      max_size: Keyword.get(opts, :max_size, 0)
    }

    Maintenance.process_maintenance(events, maint_ctx)
  end

  defp write_event(key, updates \\ 0), do: {key, {:write, key}, updates}
  defp read_event(key, updates \\ 0), do: {key, :read, updates}
  defp delete_event(key, updates \\ 0), do: {key, :delete, updates}
  defp flush_event(updates \\ 0), do: {Maintenance.flush_key(), :flush_all, updates}

  defp total_size(ctx) do
    AccessOrderDeque.size(ctx.window) +
      AccessOrderDeque.size(ctx.probation) +
      AccessOrderDeque.size(ctx.protected)
  end

  defp locate_key(ctx, key) do
    cond do
      AccessOrderDeque.member?(ctx.window, key) -> :window
      AccessOrderDeque.member?(ctx.probation, key) -> :probation
      AccessOrderDeque.member?(ctx.protected, key) -> :protected
      true -> :none
    end
  end

  defp boost_frequency(sketch, key, times \\ 10) do
    for _ <- 1..times, do: FrequencySketch.increment(sketch, key)
  end

  defp clear_deques(ctx) do
    for deque <- [ctx.window, ctx.probation, ctx.protected] do
      clear_deque(deque)
    end
  end

  defp clear_deque(deque) do
    case AccessOrderDeque.evict_lru(deque) do
      {:ok, _} -> clear_deque(deque)
      :empty -> :ok
    end
  end
end
