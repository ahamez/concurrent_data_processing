defmodule SendServer do
  use GenServer

  def init(args) do
    IO.puts("Args: #{inspect(args)}")
    max_retries = Keyword.get(args, :max_retries, 5)

    state = %{
      failed: [],
      retries: [],
      success: [],
      max_retries: max_retries
    }

    Process.send_after(self(), :retry, 5_000)

    {:ok, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast({:send, email}, state) do
    state =
      case Sender.send_email(email) do
        {:ok, :email_sent} ->
          %{state | success: [email | state.success]}

        :error ->
          %{state | retries: [%{email: email, nb_retries: 0} | state.success]}
      end

    {:noreply, state}
  end

  def handle_info(:retry, state) do
    {failed, retries, success} =
      Enum.reduce(
        state.retries,
        {state.failed, [], state.success},
        fn %{email: email, nb_retries: nb_retries}, {failed, retries, success} ->
          IO.puts("Retrying email #{email}")

          case Sender.send_email(email) do
            {:ok, :email_sent} ->
              {failed, retries, [email | success]}

            :error ->
              if nb_retries < state.max_retries do
                {failed, [%{email: email, nb_retries: nb_retries + 1}], success}
              else
                IO.puts("Failed to send email to #{email}")
                {[email | failed], retries, success}
              end
          end
        end
      )

    Process.send_after(self(), :retry, 5_000)
    {:noreply, %{state | failed: failed, retries: retries, success: success}}
  end

  def terminate(reason, _state ) do
    IO.puts("Terminating #{inspect(self())} with reason #{inspect(reason)}")
  end
end
