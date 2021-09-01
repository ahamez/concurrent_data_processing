defmodule PageConsumer do
  use GenStage
  require Logger

  def start_link(_args) do
    initial_state = []

    GenStage.start_link(__MODULE__, initial_state)
  end

  def init(initial_state) do
    Logger.info("PageConsumer init")

    subscribe_options = [{PageProducer, min_demand: 0, max_demand: 1}]
    {:consumer, initial_state, subscribe_to: subscribe_options}

    # or directly (without options)
    # {:consumer, initial_state, subscribe_to: [PageProducer]}
  end

  def handle_events(events, _from, state) do
    Logger.info("PageConsumer received events #{inspect(events)}")

    # Some dummy work
    Enum.each(events, fn _page -> Scrapper.work() end)

    {:noreply, [], state}
  end
end
