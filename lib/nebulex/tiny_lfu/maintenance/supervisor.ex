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

  # Default buffer processing interval when not given in the buffer options
  @default_processing_interval :timer.seconds(1)

  # Derived read-buffer drain threshold (Caffeine's per-stripe read capacity)
  @read_drain_threshold 64

  # Cap for the derived write-buffer drain threshold (Caffeine's
  # WRITE_BUFFER_MAX per partition)
  @max_write_drain_threshold 128

  # Derived maintenance-queue drain threshold (events are coalesced
  # upstream — drain whenever work exists)
  @queue_drain_threshold 1

  # Floor for the derived drain check interval, in milliseconds
  @min_drain_check_interval 50

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

    # Per-buffer drain defaults derived from max_size and the processing
    # interval; user-explicit drain_* values win
    %{read: r_buffer_opts, write: w_buffer_opts, queue: queue_opts} =
      derive_drain_opts(buffer_opts, max_size)

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
      {Tidefall.HashMap, [name: r_buffer_name, processor: buffer_processor] ++ r_buffer_opts},
      {Tidefall.HashMap, [name: w_buffer_name, processor: buffer_processor] ++ w_buffer_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  ## Name helpers

  @doc false
  def sup_name(name), do: camelize_and_concat([name, MaintenanceSupervisor])

  ## Drain-defaults derivation

  # Derives Caffeine-aligned per-buffer drain defaults from `max_size` and
  # the processing interval; user-explicit `drain_*` keys win and apply to
  # all three buffers. See the "Derived Drain Defaults" section in the
  # architecture guide (`guides/learning/architecture.md`) for the full
  # rationale, validated by `benchmarks/drain_tuning.exs`.
  #
  # Public (but undocumented) so the drain-tuning bench can build its config
  # grid from the real derivation instead of a copy that could drift.
  @doc false
  @spec derive_drain_opts(keyword(), pos_integer()) :: %{
          read: keyword(),
          write: keyword(),
          queue: keyword()
        }
  def derive_drain_opts(buffer_opts, max_size) do
    interval = Keyword.get(buffer_opts, :processing_interval, @default_processing_interval)
    partitions = Keyword.get(buffer_opts, :partitions, System.schedulers_online())
    check_interval = max(@min_drain_check_interval, div(interval, 10))
    write_threshold = max(1, min(@max_write_drain_threshold, div(max_size, partitions)))

    %{
      read: drain_defaults(@read_drain_threshold, check_interval, buffer_opts),
      write: drain_defaults(write_threshold, check_interval, buffer_opts),
      # Maintenance queue always uses 1 partition (single-writer guarantee)
      queue:
        @queue_drain_threshold
        |> drain_defaults(check_interval, buffer_opts)
        |> Keyword.put(:partitions, 1)
    }
  end

  # Per-buffer drain options: derived defaults overridden by any
  # user-explicit `buffer_opts`.
  defp drain_defaults(drain_threshold, check_interval, buffer_opts) do
    Keyword.merge(
      [drain_threshold: drain_threshold, drain_check_interval: check_interval],
      buffer_opts
    )
  end
end
