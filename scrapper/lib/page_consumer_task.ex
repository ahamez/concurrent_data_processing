defmodule PageConsumerTask do
  # Compare this to the original PageConsumer worker which is much more verbose.
  require Logger

  def start_link(event) do
    Logger.info("PageConsumerTask received #{inspect(event)}")

    Task.start_link(&Scrapper.work/0)
  end
end
