defmodule PageConsumerSupervisor do
  use ConsumerSupervisor
  require Logger

  @max_demand System.schedulers_online()

  def start_link(_args) do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    Logger.info("PageConsumerSupervisor init")

    children = [
      %{
        id: PageConsumerTask,
        start: {PageConsumerTask, :start_link, []},
        restart: :transient
      }
    ]

    opts = [
      strategy: :one_for_one,

      # Subscribe to a named producter
      # subscribe_to: [{OnlinePageProducerConsumer, max_demand: @max_demand}]

      # Or subscribe to producers registered via a Registry
      subscribe_to: [
        {OnlinePageProducerConsumer.via(:producer_consumer_1), max_demand: @max_demand},
        {OnlinePageProducerConsumer.via(:producer_consumer_2), max_demand: @max_demand}
      ]
    ]

    ConsumerSupervisor.init(children, opts)
  end
end
