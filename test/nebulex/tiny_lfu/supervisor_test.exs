defmodule Nebulex.TinyLFU.SupervisorTest do
  use ExUnit.Case, async: true

  alias Nebulex.TinyLFU.{AccessOrderDeque, Maintenance}
  alias Nebulex.TinyLFU.Supervisor, as: TinyLFUSupervisor

  setup do
    pid =
      start_supervised!(
        {TinyLFUSupervisor,
         name: __MODULE__, max_size: 1_000, buffer_opts: [processing_interval_ms: 100]}
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

      assert PartitionedBuffer.buffer_size(queue_name) == 0
    end

    test "starts the read buffer", %{name: name} do
      read_buffer = Maintenance.read_buffer_name(name)

      assert PartitionedBuffer.buffer_size(read_buffer) == 0
    end

    test "starts the write buffer", %{name: name} do
      write_buffer = Maintenance.write_buffer_name(name)

      assert PartitionedBuffer.buffer_size(write_buffer) == 0
    end
  end

  describe "end-to-end pipeline" do
    test "write events flow through write buffer to deques", %{name: name} do
      write_buffer = Maintenance.write_buffer_name(name)

      # Push write events into the write buffer
      for i <- 1..5 do
        PartitionedBuffer.Map.put_newer(
          write_buffer,
          "key_#{i}",
          {:write, "value_#{i}"},
          System.monotonic_time()
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
      PartitionedBuffer.Map.put_newer(
        read_buffer,
        :a,
        :read,
        System.monotonic_time()
      )

      # Wait for pipeline to process (two drain cycles)
      Process.sleep(500)

      # :a should have been touched (moved to MRU), :b is now LRU
      assert AccessOrderDeque.peek_lru(window_deque) == {:ok, :b}
    end
  end
end
