defmodule Nebulex.TinyLFU.Maintenance.Supervisor do
  @moduledoc """
  Inner supervisor for the W-TinyLFU maintenance pipeline.

  Started after the access-order deques are up. On init, fetches deque
  structs, creates the frequency sketch, and wires up MFA-based processor
  callbacks pointing to `Nebulex.TinyLFU.Maintenance`.

  Uses `:rest_for_one` strategy so that if the maintenance queue restarts,
  the read and write buffers restart too (ensuring their processor references
  a valid queue).

  ## Children

      Maintenance.Supervisor (:rest_for_one)
        ├── Maintenance Queue  (Tidefall.Queue, 1 partition)
        ├── Read Buffer        (Tidefall.HashMap, N partitions)
        └── Write Buffer       (Tidefall.HashMap, N partitions)

  """

  use Supervisor

  import Nebulex.Utils, only: [camelize_and_concat: 1]

  alias Nebulex.TinyLFU.{AccessOrderDeque, FrequencySketch, Maintenance}
  alias Nebulex.TinyLFU.Supervisor, as: TinyLFUSupervisor

  # Tidefall buffer start options we pass through from the adapter.
  # Note: `:key_hasher` is deliberately excluded — it is a Tidefall *runtime*
  # option (per put_newer/put_all_newer call), not a buffer start option, so
  # it is threaded through the adapter's hot-path writes instead.
  @buffer_option_keys [
    :processing_interval,
    :partitions,
    :processing_timeout,
    :processing_batch_size,
    :drain_threshold,
    :drain_check_interval
  ]

  ## API

  @doc """
  Starts the maintenance supervisor.

  Expects the same options as `Nebulex.TinyLFU.Supervisor.start_link/1`.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    sup_name =
      opts
      |> Keyword.fetch!(:name)
      |> sup_name()

    Supervisor.start_link(__MODULE__, opts, name: sup_name)
  end

  ## Supervisor callbacks

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    max_size = Keyword.fetch!(opts, :max_size)

    # Buffer options (already validated by the adapter)
    buffer_opts =
      opts
      |> Keyword.fetch!(:buffer_opts)
      |> Keyword.take(@buffer_option_keys)

    # Maintenance queue always uses 1 partition (single-writer guarantee)
    queue_opts = Keyword.put(buffer_opts, :partitions, 1)

    # Fetch deque structs (deques are already running)
    [window_deque, probation_deque, protected_deque] =
      TinyLFUSupervisor.deque_segments()
      |> Enum.map(fn segment ->
        name
        |> TinyLFUSupervisor.deque_name(segment)
        |> AccessOrderDeque.get_deque()
      end)

    # Create frequency sketch
    sketch = FrequencySketch.new(max_size)

    # Compute segment capacities (from Caffeine's BoundedLocalCache defaults):
    # - Window: 1% of total (PERCENT_MAIN = 0.99)
    # - Main: 99% of total
    # - Protected: 80% of main (PERCENT_MAIN_PROTECTED = 0.80)
    # - Probation: 20% of main (implicit, controlled by admission)
    # See: Caffeine's BoundedLocalCache.java
    window_max = max(1, div(max_size, 100))
    main_max = max_size - window_max
    protected_max = div(main_max * 4, 5)

    # Child names
    queue_name = Maintenance.queue_name(name)
    r_buffer_name = Maintenance.read_buffer_name(name)
    w_buffer_name = Maintenance.write_buffer_name(name)

    # Build the maintenance context struct
    maint_ctx = %Maintenance{
      sketch: sketch,
      window: window_deque,
      probation: probation_deque,
      protected: protected_deque,
      window_max: window_max,
      protected_max: protected_max,
      data_tab: Keyword.get(opts, :data_tab),
      max_size: max_size
    }

    # MFA processors — batch is prepended as first argument
    maint_processor = {Maintenance, :process_maintenance, [maint_ctx]}

    buffer_processor = {Maintenance, :process_buffer, [queue_name]}

    children = [
      {Tidefall.Queue, [name: queue_name, processor: maint_processor] ++ queue_opts},
      {Tidefall.HashMap, [name: r_buffer_name, processor: buffer_processor] ++ buffer_opts},
      {Tidefall.HashMap, [name: w_buffer_name, processor: buffer_processor] ++ buffer_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  ## Name helpers

  @doc false
  def sup_name(name), do: camelize_and_concat([name, MaintenanceSupervisor])
end
