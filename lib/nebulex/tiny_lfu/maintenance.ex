defmodule Nebulex.TinyLFU.Maintenance do
  @moduledoc """
  Processor callbacks and eviction/admission logic for the W-TinyLFU policy.

  Contains the functions invoked by `Tidefall` via MFA tuples:

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
      in place (like a read). Otherwise add to the Window deque; when the
      window exceeds its capacity, its LRU is promoted to Probation.
      TinyLFU admission runs later, only under capacity pressure (see
      `process_maintenance/2`).
    * `{key, :delete, _updates}` — remove the key from whichever deque
      it's in.
    * `{_key, :flush_all, _updates}` — whole-cache invalidation: wipe all
      three deques and zero the frequency sketch. Emitted by the adapter
      when `delete_all/1` clears the cache.

  """

  import Nebulex.Utils, only: [camelize_and_concat: 1]

  alias Nebulex.TinyLFU.{AccessOrderDeque, FrequencySketch}
  alias Tidefall.HashMap.Entry

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

  ## Processor callbacks (called via MFA by Tidefall)

  @doc false
  @spec process_buffer([Entry.t()], atom()) :: :ok
  def process_buffer(batch, queue_name) do
    # Forward the buffer's `updates` counter so the maintenance worker can
    # tick the frequency sketch once per coalesced event. Drop `version`;
    # the maintenance pipeline doesn't use it. `Entry.key` is always the
    # original (pre-hash) key, so the queue sees user keys regardless of the
    # configured `:key_hasher`.
    batch
    |> Enum.map(fn %Entry{key: key, value: value, updates: updates} -> {key, value, updates} end)
    |> then(&Tidefall.Queue.push(queue_name, &1))
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

    # Window-overflow promotions are collected (in promotion order) as this
    # batch's admission candidates — Caffeine's evictFromMain() evaluates
    # exactly the entries evictFromWindow() just promoted.
    candidates =
      other_events
      |> Enum.reduce([], fn event, acc ->
        case process_event(event, ctx) do
          {:candidate, key} -> [key | acc]
          _other -> acc
        end
      end)
      |> Enum.reverse()

    # After processing the batch, enforce total cache size by evicting
    # entries until total_size <= max_size. Matches Caffeine's evictFromMain()
    # with a `while (weightedSize() > maximum())` loop — including the
    # candidate-vs-victim admission filter, which only runs here, under
    # capacity pressure.
    if ctx.max_size > 0 do
      evict_entries(ctx, candidates)
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

  # Promotes the window LRU into probation (at MRU) when the window exceeds
  # capacity. Promotion is unconditional — matches Caffeine's
  # evictFromWindow(), which moves overflow into probation without any
  # admission check; the TinyLFU filter only runs later, under capacity
  # pressure, in evict_entries/2. Returns the promoted key so the caller can
  # collect it as an admission candidate for this batch.
  #
  # Note the `>` (not `>=`): the window briefly holds `window_max + 1`
  # entries between the new key being added and this promotion step. Treat
  # `window_max` as a soft cap — the deque is never larger than that for
  # more than one event's worth of work in the maintenance batch.
  defp evict_from_window(ctx) do
    if AccessOrderDeque.size(ctx.window) > ctx.window_max do
      {:ok, candidate} = AccessOrderDeque.evict_lru(ctx.window)

      AccessOrderDeque.put(ctx.probation, candidate)

      {:candidate, candidate}
    else
      :ok
    end
  end

  # Enforces total cache size after processing a batch, mirroring Caffeine's
  # evictFromMain(): while over capacity, this batch's window-promoted
  # candidates (in promotion order) are compared against the probation LRU
  # (the victim) via the frequency sketch, evicting whichever is less worthy.
  # Once candidates are exhausted, plain LRU eviction applies — probation
  # first, then protected, then window.
  defp evict_entries(ctx, candidates) do
    total =
      AccessOrderDeque.size(ctx.window) +
        AccessOrderDeque.size(ctx.probation) +
        AccessOrderDeque.size(ctx.protected)

    evict_entries(ctx, candidates, total)
  end

  # Every successful evict_one/2 removes exactly one entry across the three
  # deques, so the running total is decremented instead of re-reading the
  # three deque sizes (ETS info calls) on each iteration.
  defp evict_entries(ctx, candidates, total) do
    if total > ctx.max_size do
      case evict_one(ctx, candidates) do
        {:cont, candidates} -> evict_entries(ctx, candidates, total - 1)
        :halt -> :ok
      end
    else
      :ok
    end
  end

  defp evict_one(ctx, candidates) do
    case next_candidate(ctx, candidates) do
      {candidate, rest} -> {:cont, admit_or_evict(candidate, rest, ctx)}
      :none -> evict_via_lru(ctx)
    end
  end

  # No candidates left — evict the plain LRU victim. A `nil` victim (all
  # deques empty) is unreachable while total > max_size, but halt instead
  # of looping if it ever happens.
  defp evict_via_lru(ctx) do
    case evict_victim(ctx) do
      nil -> :halt
      _key -> {:cont, []}
    end
  end

  # Next batch candidate still in probation. A candidate may have been
  # deleted, or promoted to protected by a later event in the same batch —
  # skip those.
  defp next_candidate(_ctx, []) do
    :none
  end

  defp next_candidate(ctx, [candidate | rest]) do
    if AccessOrderDeque.member?(ctx.probation, candidate) do
      {candidate, rest}
    else
      next_candidate(ctx, rest)
    end
  end

  # TinyLFU admission under capacity pressure: evict the victim (probation
  # LRU) when the candidate's frequency is strictly greater; otherwise evict
  # the candidate (ties retain the victim, matching Caffeine). Either way the
  # candidate is consumed — Caffeine advances the candidate pointer on both
  # admit and reject. When the probation LRU is the candidate itself, the
  # self-comparison falls into the reject branch and evicts the candidate.
  defp admit_or_evict(candidate, rest, ctx) do
    # Probation is non-empty: `candidate` is a member (see next_candidate/2)
    {:ok, victim} = AccessOrderDeque.peek_lru(ctx.probation)

    if FrequencySketch.frequency(ctx.sketch, candidate) >
         FrequencySketch.frequency(ctx.sketch, victim) do
      {:ok, ^victim} = AccessOrderDeque.remove(ctx.probation, victim)

      maybe_delete(ctx.data_tab, victim)
    else
      {:ok, ^candidate} = AccessOrderDeque.remove(ctx.probation, candidate)

      maybe_delete(ctx.data_tab, candidate)
    end

    rest
  end

  # Plain LRU eviction: probation first, then protected, then window.
  defp evict_victim(ctx) do
    evicted =
      case AccessOrderDeque.evict_lru(ctx.probation) do
        {:ok, key} -> key
        :empty -> evict_fallback(ctx.protected, ctx.window)
      end

    if evicted do
      maybe_delete(ctx.data_tab, evicted)
    end

    evicted
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

      # Unreachable in practice: `evict_victim/1` only descends into
      # `evict_fallback_window` when probation and protected are empty,
      # and at that point the loop guard `total > max_size` (with
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
