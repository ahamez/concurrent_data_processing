defmodule PartitionDispatcherConsumer do
  use GenStage
  require Logger

  def start_link(partition) do
    state = %{partition: partition}

    GenStage.start_link(__MODULE__, state)
  end

  # Callbacks

  def init(state) do
    Logger.info("#{__MODULE__} with partition #{state.partition}")

    subscribe_options = [
      {PartitionDispatcherProducer, partition: state.partition}
    ]

    {:consumer, state, subscribe_to: subscribe_options}
  end

  def handle_events(events, _from, state) do
    Logger.info("#{__MODULE__} (#{state.partition}) received events #{inspect(events)}")

    {:noreply, [], state}
  end
end
