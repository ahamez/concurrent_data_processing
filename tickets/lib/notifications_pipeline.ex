defmodule NotificationsPipeline do
  use Broadway

  require Logger

  @producer BroadwayRabbitMQ.Producer
  @producer_config [
    queue: "notifications_queue",
    declare: [durable: true],
    on_failure: :reject_and_requeue,
    qos: [prefetch_count: 100]
  ]

  def start_link(_args) do
    options = [
      name: NotificationsPipeline,
      producer: [module: {@producer, @producer_config}],
      processors: [
        default: []
      ],
      batchers: [
        email: [concurrency: 5, batch_timeout: 10_000],
        sms: [concurrency: 5, batch_timeout: 10_000]
      ]
    ]

    Broadway.start_link(__MODULE__, options)
  end

  @impl true
  def prepare_messages(messages, _context) do
    Enum.map(messages, fn msg ->
      Broadway.Message.update_data(msg, fn data ->
        [type, recipient] = String.split(data, ",")

        %{type: type, recipient: recipient}
      end)
    end)
  end

  @impl true
  def handle_message(_processor, message, _context) do
    message
    |> Broadway.Message.put_batcher(batcher(message.data.type))
    # |> Broadway.Message.put_batcher(:email)
    |> Broadway.Message.put_batch_key(message.data.recipient)
  end

  @impl true
  def handle_batch(:email, messages, batch_info, _context) do
    Logger.info(
      "[Notifications] #{inspect(self())} Batch #{batch_info.batcher} #{batch_info.batch_key}"
    )

    messages
  end

  @impl true
  def handle_batch(:sms, messages, batch_info, _context) do
    Logger.info(
      "[Notifications] #{inspect(self())} Batch #{batch_info.batcher} #{batch_info.batch_key}"
    )

    messages
  end

  # -- Private

  defp batcher("email"), do: :email
  defp batcher("sms"), do: :sms
end
