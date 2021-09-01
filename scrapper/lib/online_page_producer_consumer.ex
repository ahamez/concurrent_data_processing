defmodule OnlinePageProducerConsumer do
  use GenStage
  require Logger

  def start_link(id) do
    state = %{id: id}

    GenStage.start_link(__MODULE__, state, name: via(id))
  end

  def init(state) do
    # We could also lookup the id using the registry.
    # [id] = Registry.keys(OnlinePageProducerConsumerRegistry, self())
    Logger.info("OnlinePageProducerConsumer #{state.id} init")

    subscription = [
      {PageProducer, min_demand: 0, max_demand: 1}
    ]

    {:producer_consumer, state, subscribe_to: subscription}
  end

  def handle_events(events, _from, state) do
    Logger.info("OnlinePageProducerConsumer #{state.id} received events #{inspect(events)}")
    events = Enum.filter(events, &Scrapper.online?/1)

    {:noreply, events, state}
  end

  def via(id) do
    {:via, Registry, {OnlinePageProducerConsumerRegistry, id}}
  end
end
