defmodule Jobber.Job do
  use GenServer, restart: :transient
  require Logger

  defstruct [:work, :id, :max_retries, retries: 0, status: :new]

  def start_link(args) do
    id = random_id()
    type = Keyword.get(args, :type)

    GenServer.start_link(__MODULE__, args, name: via(id, type))
  end

  def init(args) do
    work = Keyword.fetch!(args, :work)
    max_retries = Keyword.get(args, :max_retries, 3)

    state = %__MODULE__{id: args[:id], work: work, max_retries: max_retries}

    {:ok, state, {:continue, :run}}
  end

  def handle_continue(:run, state) do
    state = state.work.() |> handle_job_result(state)

    if state.status == :errored do
      Process.send_after(self(), :retry, 5_000)

      {:noreply, state}
    else
      Logger.info("Job exiting #{state.id}")

      {:stop, :normal, state}
    end
  end

  def handle_info(:retry, state) do
    {:noreply, state, {:continue, :run}}
  end

  # Private

  defp via(key, value) do
    {:via, Registry, {Jobber.JobRegistry, key, value}}
  end

  defp handle_job_result({:ok, _data}, state) do
    Logger.info("Job completed #{state.id}")

    %__MODULE__{state | status: :done}
  end

  defp handle_job_result(:error, %{status: :new} = state) do
    Logger.warn("Job errored #{state.id}")

    %__MODULE__{state | status: :errored}
  end

  defp handle_job_result(:error, %{status: :errored} = state) do
    Logger.warn("Job retry failed #{state.id}")
    state = %__MODULE__{state | retries: state.retries + 1}

    if state.retries == state.max_retries do
      %__MODULE__{state | status: :failed}
    else
      state
    end
  end

  defp random_id() do
    :crypto.strong_rand_bytes(5) |> Base.url_encode64(padding: false)
  end
end
