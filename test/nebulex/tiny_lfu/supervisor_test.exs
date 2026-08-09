defmodule Nebulex.TinyLFU.SupervisorTest do
  use ExUnit.Case, async: true

  alias Nebulex.TinyLFU.{AccessOrderDeque, Maintenance}
  alias Nebulex.TinyLFU.Supervisor, as: TinyLFUSupervisor

  setup do
    pid =
      start_supervised!(
        {TinyLFUSupervisor,
         name: __MODULE__, max_size: 1_000, buffer_opts: [processing_interval: 100]}
      )

    %{name: __MODULE__, sup_pid: pid}
  end

  describe "start_link/1" do
    test "starts the supervisor tree", %{sup_pid: pid} do
      assert Process.alive?(pid)
    end

    test "starts all deque children", %{name: name} do
      for segment <- TinyLFUSupervisor.deque_segments() do
        deque_name = TinyLFUSupervisor.deque_name(name, segment)
        deque = AccessOrderDeque.get_deque(deque_name)

        assert is_struct(deque, AccessOrderDeque)
        assert AccessOrderDeque.size(deque) == 0
      end
    end

    test "starts the maintenance supervisor", %{name: name} do
      sup_name = Maintenance.Supervisor.sup_name(name)

      assert Process.whereis(sup_name) |> Process.alive?()
    end

    test "starts the maintenance queue", %{name: name} do
      queue_name = Maintenance.queue_name(name)

      assert Tidefall.Buffer.buffer_size(queue_name) == 0
    end

    test "starts the read buffer", %{name: name} do
      read_buffer = Maintenance.read_buffer_name(name)

      assert Tidefall.Buffer.buffer_size(read_buffer) == 0
    end

    test "starts the write buffer", %{name: name} do
      write_buffer = Maintenance.write_buffer_name(name)

      assert Tidefall.Buffer.buffer_size(write_buffer) == 0
    end
  end

  describe "derived drain defaults" do
    # The supervisor derives Caffeine-aligned per-buffer drain defaults from
    # max_size and the processing interval (see benchmarks/RESULTS.md).
    # Assertions inspect the running Tidefall partitions, so they cover the
    # options actually in effect, not just the derivation function.

    test "bounded cache derives differentiated per-buffer thresholds" do
      name = Module.concat(__MODULE__, DerivedDefaults)

      start_supervised!(
        {TinyLFUSupervisor,
         name: name, max_size: 100_000, buffer_opts: [processing_interval: 1_000, partitions: 2]},
        id: name
      )

      # Write buffer: max(1, min(128, div(100_000, 2))) = 128
      assert drain_settings(Maintenance.write_buffer_name(name)) == [{128, 100}, {128, 100}]

      # Read buffer: 64
      assert drain_settings(Maintenance.read_buffer_name(name)) == [{64, 100}, {64, 100}]

      # Maintenance queue: 1 (single partition)
      assert drain_settings(Maintenance.queue_name(name)) == [{1, 100}]
    end

    test "small max_size caps the write-buffer threshold" do
      name = Module.concat(__MODULE__, SmallCache)

      start_supervised!(
        {TinyLFUSupervisor,
         name: name, max_size: 10, buffer_opts: [processing_interval: 1_000, partitions: 2]},
        id: name
      )

      # max(1, min(128, div(10, 2))) = 5
      assert drain_settings(Maintenance.write_buffer_name(name)) == [{5, 100}, {5, 100}]
    end

    test "drain_check_interval derives from processing_interval with a 50ms floor" do
      name = Module.concat(__MODULE__, FastInterval)

      start_supervised!(
        {TinyLFUSupervisor,
         name: name, max_size: 1_000, buffer_opts: [processing_interval: 100, partitions: 1]},
        id: name
      )

      # max(50, div(100, 10)) = 50
      assert drain_settings(Maintenance.queue_name(name)) == [{1, 50}]
    end

    test "explicit drain_threshold wins over the derived defaults for all buffers" do
      name = Module.concat(__MODULE__, ExplicitThreshold)

      start_supervised!(
        {TinyLFUSupervisor,
         name: name,
         max_size: 100_000,
         buffer_opts: [processing_interval: 1_000, partitions: 1, drain_threshold: 7]},
        id: name
      )

      # User threshold applies to all three buffers; check interval stays derived
      assert drain_settings(Maintenance.read_buffer_name(name)) == [{7, 100}]
      assert drain_settings(Maintenance.write_buffer_name(name)) == [{7, 100}]
      assert drain_settings(Maintenance.queue_name(name)) == [{7, 100}]
    end

    test "explicit drain_check_interval wins over the derived default" do
      name = Module.concat(__MODULE__, ExplicitCheckInterval)

      start_supervised!(
        {TinyLFUSupervisor,
         name: name,
         max_size: 1_000,
         buffer_opts: [processing_interval: 1_000, partitions: 1, drain_check_interval: 250]},
        id: name
      )

      # Thresholds stay derived; the user's check interval applies
      assert drain_settings(Maintenance.read_buffer_name(name)) == [{64, 250}]
      assert drain_settings(Maintenance.queue_name(name)) == [{1, 250}]
    end
  end

  describe "end-to-end pipeline" do
    test "write events flow through write buffer to deques", %{name: name} do
      write_buffer = Maintenance.write_buffer_name(name)

      # Push write events into the write buffer
      for i <- 1..5 do
        Tidefall.HashMap.put_newer(
          write_buffer,
          "key_#{i}",
          {:write, "value_#{i}"},
          version: System.monotonic_time()
        )
      end

      # Wait for buffer processing intervals to drain through the pipeline:
      # write buffer → maintenance queue → deque updates
      # Two drain cycles with processing_interval_ms=100
      Process.sleep(500)

      # Check that keys ended up in the deques
      window_deque =
        name
        |> TinyLFUSupervisor.deque_name(WindowDeque)
        |> AccessOrderDeque.get_deque()

      probation_deque =
        name
        |> TinyLFUSupervisor.deque_name(ProbationDeque)
        |> AccessOrderDeque.get_deque()

      total =
        AccessOrderDeque.size(window_deque) +
          AccessOrderDeque.size(probation_deque)

      assert total > 0, "Expected keys to be in deques after pipeline processing"
    end

    test "read events flow through read buffer to touch deques", %{name: name} do
      read_buffer = Maintenance.read_buffer_name(name)

      window_deque =
        name
        |> TinyLFUSupervisor.deque_name(WindowDeque)
        |> AccessOrderDeque.get_deque()

      # Pre-populate the window deque directly
      AccessOrderDeque.put(window_deque, :a)
      AccessOrderDeque.put(window_deque, :b)
      assert AccessOrderDeque.peek_lru(window_deque) == {:ok, :a}

      # Push a read event for :a (should touch it, moving to MRU)
      Tidefall.HashMap.put_newer(
        read_buffer,
        :a,
        :read,
        version: System.monotonic_time()
      )

      # Wait for pipeline to process (two drain cycles)
      Process.sleep(500)

      # :a should have been touched (moved to MRU), :b is now LRU
      assert AccessOrderDeque.peek_lru(window_deque) == {:ok, :b}
    end
  end

  ## Helpers

  # Reads {drain_threshold, drain_check_interval} from each running Tidefall
  # partition of the given buffer.
  defp drain_settings(buffer) do
    Tidefall.Registry
    |> Registry.lookup(buffer)
    |> Enum.map(fn {pid, _partition} ->
      state = :sys.get_state(pid)

      {state.drain_threshold, state.drain_check_interval}
    end)
    |> Enum.sort()
  end
end
