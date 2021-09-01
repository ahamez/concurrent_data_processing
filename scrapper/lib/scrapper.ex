defmodule Scrapper do
  @moduledoc false

  def online?(_url) do
    work()

    Enum.random([false, true, true, true])
  end

  # Simulate some heavy work
  def work() do
    1..5
    |> Enum.random()
    |> :timer.seconds()
    |> Process.sleep()
  end
end
