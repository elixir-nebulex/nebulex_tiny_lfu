defmodule Nebulex.TinyLFU.Maintenance do
  @moduledoc """
  Processor callbacks and eviction/admission logic for the W-TinyLFU policy.

  Contains the functions invoked by `PartitionedBuffer` via MFA tuples:

    * `process_buffer/2` — drains read/write buffer entries and pushes
      them to the maintenance queue.
    * `process_maintenance/2` — processes maintenance queue events,
      updating deques and running the TinyLFU admission policy.

  ## Event Types

  Events flow into `process_maintenance/2` as `{key, value, updates}`
  triples, where `updates` is the count of additional `put_newer` calls
  that the buffer coalesced into the event (so the total event count is
  `updates + 1`). Most handlers use the count to drive the frequency
  sketch so a bursty hot key isn't undercounted just because the buffer
  deduplicated repeated events for it.

    * `{key, :read, updates}` — increment the frequency sketch by
      `updates + 1` via `FrequencySketch.increment/3` (one bulk call,
      naturally capped at the sketch's saturation point), then touch the
      key in its current deque segment; promote from Probation to
      Protected on access. Matches Caffeine's `onAccess()`.
    * `{key, {:write, value}, updates}` — increment the frequency sketch
      by `updates + 1`. If the key already exists in a deque, touch it
      in place (like a read). Otherwise add to the Window deque; run
      admission if Window is full.
    * `{key, :delete, _updates}` — remove the key from whichever deque
      it's in.
    * `{_key, :flush_all, _updates}` — whole-cache invalidation: wipe all
      three deques and zero the frequency sketch. Emitted by the adapter
      when `delete_all/1` clears the cache.

  """

  import Nebulex.Utils, only: [camelize_and_concat: 1]

  alias Nebulex.TinyLFU.{AccessOrderDeque, FrequencySketch}

  # Holds the frequency sketch, deque references, segment capacities,
  # and optional ETS data table for the maintenance processor.
  @enforce_keys [:sketch, :window, :probation, :protected, :window_max, :protected_max]
  defstruct [
    :sketch,
    :window,
    :probation,
    :protected,
    :window_max,
    :protected_max,
    data_tab: nil,
    max_size: 0
  ]

  @typedoc "Processing context for the W-TinyLFU maintenance loop."
  @type t :: %__MODULE__{
          sketch: FrequencySketch.t(),
          window: AccessOrderDeque.t(),
          probation: AccessOrderDeque.t(),
          protected: AccessOrderDeque.t(),
          window_max: pos_integer(),
          protected_max: pos_integer(),
          data_tab: :ets.tid() | nil,
          max_size: non_neg_integer()
        }

  # Sentinel key used to deliver whole-cache invalidation events through the
  # write buffer. Picked to be unlikely to collide with user keys.
  @flush_key :__nbx_tinylfu_flush__

  @doc false
  def flush_key, do: @flush_key

  ## Name helpers

  @doc false
  def queue_name(name), do: camelize_and_concat([name, Queue])

  @doc false
  def read_buffer_name(name), do: camelize_and_concat([name, ReadBuffer])

  @doc false
  def write_buffer_name(name), do: camelize_and_concat([name, WriteBuffer])

  ## Processor callbacks (called via MFA by PartitionedBuffer)

  @doc false
  @spec process_buffer([{any(), any(), any(), any()}], atom()) :: :ok
  def process_buffer(batch, queue_name) do
    # Forward the buffer's `updates` counter so the maintenance worker can
    # tick the frequency sketch once per coalesced event. Drop `version`;
    # the maintenance pipeline doesn't use it.
    batch
    |> Enum.map(fn {key, value, _version, updates} -> {key, value, updates} end)
    |> then(&PartitionedBuffer.Queue.push(queue_name, &1))
  end

  @doc false
  @spec process_maintenance([any()], t()) :: :ok
  def process_maintenance(batch, %__MODULE__{} = ctx) do
    # Flush events must be processed before any other events in the same
    # batch. Otherwise, a write that happens to be ordered after the flush
    # in batch order would be re-added to the deques after the wipe,
    # leaving a phantom that ETS no longer has.
    {flush_events, other_events} =
      Enum.split_with(batch, fn
        {_key, :flush_all, _updates} -> true
        _ -> false
      end)

    :ok = Enum.each(flush_events, &process_event(&1, ctx))
    :ok = Enum.each(other_events, &process_event(&1, ctx))

    # After processing the batch, enforce total cache size by evicting
    # entries until total_size <= max_size. Matches Caffeine's evictFromMain()
    # with a `while (weightedSize() > maximum())` loop.
    if ctx.data_tab && ctx.max_size > 0 do
      evict_entries(ctx)
    end

    :ok
  end

  ## Event processing

  defp process_event({key, :read, updates}, ctx) do
    # Replay the coalesced events against the sketch (the buffer's
    # `updates` counter records *additional* puts, so total = updates + 1).
    # Matches Caffeine's onAccess, which sees every access losslessly.
    FrequencySketch.increment(ctx.sketch, key, updates + 1)

    on_access(key, ctx)
  end

  defp process_event({key, {:write, _value}, updates}, ctx) do
    # Stale write guard: if the data table is set and the key isn't in ETS,
    # the entry was deleted (e.g. by `delete_all` on the hot path) after
    # this event was scheduled but before the buffer drained. Drop the
    # write to avoid resurrecting a phantom in the deques.
    if ctx.data_tab && not :ets.member(ctx.data_tab, key) do
      :ok
    else
      # Replay the coalesced events against the sketch.
      FrequencySketch.increment(ctx.sketch, key, updates + 1)

      # If key already exists in any deque, touch it in place (like Caffeine's
      # UpdateTask → onAccess). Only new keys enter the window deque.
      if key_exists?(key, ctx) do
        on_access(key, ctx)
      else
        # New key — add to window
        AccessOrderDeque.put(ctx.window, key)

        # Evict from window if full
        evict_from_window(ctx)
      end
    end
  end

  defp process_event({_key, :flush_all, _updates}, ctx) do
    # Whole-cache invalidation: ETS was already cleared on the hot path by
    # `delete_all/1`. Wipe the policy state so the next admission isn't
    # comparing candidates against ghosts of evicted entries.
    :ok = AccessOrderDeque.clear(ctx.window)
    :ok = AccessOrderDeque.clear(ctx.probation)
    :ok = AccessOrderDeque.clear(ctx.protected)
    :ok = FrequencySketch.clear(ctx.sketch)
  end

  defp process_event({key, :delete, _updates}, ctx) do
    # Remove from whichever deque the key is in
    with :error <- AccessOrderDeque.remove(ctx.window, key),
         :error <- AccessOrderDeque.remove(ctx.probation, key) do
      AccessOrderDeque.remove(ctx.protected, key)
    end

    :ok
  end

  ## Internal helpers

  defp key_exists?(key, ctx) do
    AccessOrderDeque.member?(ctx.window, key) or
      AccessOrderDeque.member?(ctx.probation, key) or
      AccessOrderDeque.member?(ctx.protected, key)
  end

  # Matches Caffeine's onAccess(): reorder the key within its current deque,
  # or promote from probation to protected.
  defp on_access(key, ctx) do
    cond do
      AccessOrderDeque.member?(ctx.window, key) ->
        # Touch in window — move to MRU
        AccessOrderDeque.put(ctx.window, key)

      AccessOrderDeque.member?(ctx.probation, key) ->
        promote_to_protected(key, ctx)

      AccessOrderDeque.member?(ctx.protected, key) ->
        # Touch in protected — move to MRU
        AccessOrderDeque.put(ctx.protected, key)

      true ->
        # Key not in any deque (write not processed yet) — skip
        :ok
    end
  end

  # Promotes a key from probation to protected, demoting the protected
  # LRU back to probation if protected is full.
  defp promote_to_protected(key, ctx) do
    AccessOrderDeque.remove(ctx.probation, key)
    AccessOrderDeque.put(ctx.protected, key)

    if AccessOrderDeque.size(ctx.protected) > ctx.protected_max do
      {:ok, demoted} = AccessOrderDeque.evict_lru(ctx.protected)

      AccessOrderDeque.put(ctx.probation, demoted)
    end
  end

  # Evicts the LRU entry from window if it exceeds capacity, then runs
  # the TinyLFU admission policy against probation's LRU.
  #
  # Note the `>` (not `>=`): the window briefly holds `window_max + 1`
  # entries between the new key being added and this eviction step. Treat
  # `window_max` as a soft cap — the deque is never larger than that for
  # more than one event's worth of work in the maintenance batch.
  defp evict_from_window(ctx) do
    if AccessOrderDeque.size(ctx.window) > ctx.window_max do
      {:ok, window_victim} = AccessOrderDeque.evict_lru(ctx.window)

      admit_or_discard(window_victim, ctx)
    end
  end

  # Admission: compare window victim vs probation LRU.
  defp admit_or_discard(window_victim, ctx) do
    case AccessOrderDeque.peek_lru(ctx.probation) do
      {:ok, main_victim} ->
        admit_candidate(window_victim, main_victim, ctx)

      :empty ->
        # Probation is empty — admit directly
        AccessOrderDeque.put(ctx.probation, window_victim)
    end
  end

  defp admit_candidate(candidate, victim, ctx) do
    if FrequencySketch.frequency(ctx.sketch, candidate) >
         FrequencySketch.frequency(ctx.sketch, victim) do
      # Admit candidate to probation, evict victim
      {:ok, ^victim} = AccessOrderDeque.evict_lru(ctx.probation)

      AccessOrderDeque.put(ctx.probation, candidate)
      maybe_delete(ctx.data_tab, victim)
    else
      # Discard candidate
      maybe_delete(ctx.data_tab, candidate)
    end
  end

  # Enforces total cache size after processing a batch. Evicts from
  # probation LRU first, then protected LRU if probation is exhausted.
  # Matches Caffeine's evictFromMain() loop.
  defp evict_entries(ctx) do
    total =
      AccessOrderDeque.size(ctx.window) +
        AccessOrderDeque.size(ctx.probation) +
        AccessOrderDeque.size(ctx.protected)

    if total > ctx.max_size do
      evicted =
        case AccessOrderDeque.evict_lru(ctx.probation) do
          {:ok, key} -> key
          :empty -> evict_fallback(ctx.protected, ctx.window)
        end

      if evicted do
        maybe_delete(ctx.data_tab, evicted)

        evict_entries(ctx)
      end
    end
  end

  # Fallback eviction order: protected LRU, then window LRU
  defp evict_fallback(protected_deque, window_deque) do
    case AccessOrderDeque.evict_lru(protected_deque) do
      {:ok, key} -> key
      :empty -> evict_fallback_window(window_deque)
    end
  end

  defp evict_fallback_window(window_deque) do
    case AccessOrderDeque.evict_lru(window_deque) do
      {:ok, key} ->
        key

      # Unreachable in practice: `evict_entries/1` only descends into
      # `evict_fallback_window` when probation and protected are empty,
      # and at that point the recursion guard `total > max_size` (with
      # `max_size >= 1` since it's a `:pos_integer`) requires window to
      # hold at least one entry. The clause exists purely as a safety
      # net so a bug in the calling path can't crash the maintenance
      # worker.
      # coveralls-ignore-next-line
      :empty ->
        nil
    end
  end

  # Deletes a key from the ETS data table if one is configured.
  defp maybe_delete(nil, _key), do: :ok
  defp maybe_delete(data_tab, key), do: :ets.delete(data_tab, key)
end
