defmodule Nebulex.TinyLFU.Supervisor do
  @moduledoc """
  Top-level supervisor for the W-TinyLFU cache components.

  Starts the three access-order deques (Window, Probation, Protected) and
  the maintenance supervisor that manages the buffer pipeline.

  Uses `:rest_for_one` strategy so that if a deque restarts, the maintenance
  supervisor also restarts (re-fetching fresh deque references).

  ## Children

      Nebulex.TinyLFU.Supervisor (:rest_for_one)
        ├── AccessOrderDeque  (Window)
        ├── AccessOrderDeque  (Probation)
        ├── AccessOrderDeque  (Protected)
        └── Nebulex.TinyLFU.Maintenance.Supervisor (:rest_for_one)
              ├── Maintenance Queue  (PB.Queue, 1 partition)
              ├── Read Buffer        (PB.Map, N partitions)
              └── Write Buffer       (PB.Map, N partitions)

  """

  use Supervisor

  import Nebulex.Utils, only: [camelize_and_concat: 1]

  alias Nebulex.TinyLFU.{AccessOrderDeque, Maintenance}

  # Segment names
  @deque_segments [WindowDeque, ProbationDeque, ProtectedDeque]

  ## API

  @doc """
  Starts the TinyLFU supervisor.

  ## Options

    * `:name` - (required) the cache name, used to derive child process names.
    * `:max_size` - (required) total cache capacity.

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)

    Supervisor.start_link(__MODULE__, opts, name: sup_name(name))
  end

  ## Supervisor callbacks

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    deque_children =
      for segment <- @deque_segments do
        deque_name = deque_name(name, segment)

        Supervisor.child_spec(
          {AccessOrderDeque, [name: deque_name]},
          id: deque_name
        )
      end

    children = deque_children ++ [{Maintenance.Supervisor, opts}]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  ## Internal helpers

  @doc false
  @spec deque_name(atom(), atom()) :: atom()
  def deque_name(name, segment) when segment in @deque_segments do
    camelize_and_concat([name, segment])
  end

  @doc false
  @spec sup_name(atom()) :: atom()
  def sup_name(name), do: camelize_and_concat([name, TinyLFUSupervisor])

  @doc false
  def deque_segments, do: @deque_segments
end
