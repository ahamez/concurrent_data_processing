defmodule Scrapper.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Children need to be started in the same order as the data flow one.
    children = [
      PageProducer,

      # Registry to register producer-consumers. Will be used by PageConsumerSupervisor
      # to retrieve producers to subscribe to.
      {Registry, keys: :unique, name: OnlinePageProducerConsumerRegistry},
      # Producer-consumers registered with the above registry.
      online_page_producer_consumer_spec(id: :producer_consumer_1),
      online_page_producer_consumer_spec(id: :producer_consumer_2),

      # Using a supervisor that will take care of PageConsumerTask workers
      PageConsumerSupervisor,

      # Or, spawn them here (note that it's using the PageConsumer module)
      # Supervisor.child_spec(PageConsumer, id: :consumer_1),
      # Supervisor.child_spec(PageConsumer, id: :consumer_2)

      PartitionDispatcherProducer,

      # There can be only one consumer per partition
      Supervisor.child_spec({PartitionDispatcherConsumer, :a}, id: :partition_consumer_1),
      Supervisor.child_spec({PartitionDispatcherConsumer, :b}, id: :partition_consumer_2)
    ]

    opts = [strategy: :one_for_one, name: Scrapper.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp online_page_producer_consumer_spec(id: id) do
    Supervisor.child_spec({OnlinePageProducerConsumer, id}, id: id)
  end
end
