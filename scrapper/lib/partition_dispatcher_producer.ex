defmodule PartitionDispatcherProducer do
  use GenStage
  require Logger

  def start_link(_args) do
    state = []

    GenStage.start_link(__MODULE__, state, name: __MODULE__)
  end

  def foo(data) do
    GenStage.cast(__MODULE__, {:foo, data})
  end

  # Callbacks

  def init(state) do
    Logger.info("PartitionDispatcherProducer init")

    hash = fn
      :a -> {:a, :a}
      event -> {event, :b}
    end

    opts = [
      partitions: [:a, :b],
      hash: hash
    ]

    {:producer, state, dispatcher: {GenStage.PartitionDispatcher, opts}}
  end

  def handle_cast({:foo, data}, state) do
    {:noreply, data, state}
  end

  def handle_demand(demand, state) do
    Logger.info("#{__MODULE__} received demand: #{demand}")
    events = []

    {:noreply, events, state}
  end
end
