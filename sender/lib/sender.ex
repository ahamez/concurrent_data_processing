defmodule Sender do
  @moduledoc false
  def send_email("nope@nay") do
    :error
  end

  def send_email(email = "hello@world") do
    Process.sleep(4_000)
    IO.puts("Email to #{email} sent")

    {:ok, :email_sent}
  end

  def send_email(email) do
    Process.sleep(2_000)
    IO.puts("Email to #{email} sent")

    {:ok, :email_sent}
  end

  def notify_all(emails) do
    Sender.EmailTaskSupervisor
    |> Task.Supervisor.async_stream_nolink(emails, &send_email/1, ordered: false)
    |> Enum.to_list()
  end
end

defmodule Benchmark do
  def measure(function) do
    function
    |> :timer.tc()
    |> elem(0)
    |> Kernel./(1_000_000)
  end
end
