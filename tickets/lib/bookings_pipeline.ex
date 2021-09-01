defmodule BookingsPipeline do
  use Broadway

  require Logger

  @producer BroadwayRabbitMQ.Producer
  @producer_config [
    queue: "bookings_queue",
    declare: [durable: true],
    on_failure: :reject_and_requeue
  ]

  def start_link(_args) do
    options = [
      name: __MODULE__,
      producer: [
        module: {@producer, @producer_config}
        # concurrency: 1
      ],
      processors: [
        default: [
          # concurrency: System.schedulers_online() * 2
        ]
      ],
      batchers: [
        cinema: [],
        musical: [],
        default: []
      ]
    ]

    Broadway.start_link(__MODULE__, options)
  end

  @impl true
  def prepare_messages(messages, _context) do
    messages =
      Enum.map(messages, fn msg ->
        Broadway.Message.update_data(msg, fn data ->
          [event, user_id] = String.split(data, ",")

          %{event: event, user_id: user_id}
        end)
      end)

    users =
      messages
      |> Enum.map(& &1.data.user_id)
      |> Tickets.users_by_ids()

    # Put users in messages
    Enum.map(messages, fn msg ->
      Broadway.Message.update_data(msg, fn data ->
        user = Enum.find(users, &(&1.id == data.user_id))

        Map.put(data, :user, user)
      end)
    end)
  end

  @impl true
  def handle_failed(messages, _context) do
    Logger.warn("[Bookings] #{length(messages)} failed")

    Enum.map(messages, fn
      %{status: {:failed, "bookings-closed"}} = msg ->
        Broadway.Message.configure_ack(msg, on_failure: :reject)

      msg ->
        msg
    end)
  end

  @impl true
  def handle_message(_processor, message, _context) do
    if Tickets.tickets_available?(message.data.event) do
      case message do
        %{data: %{event: "cinema"}} -> Broadway.Message.put_batcher(message, :cinema)
        %{data: %{event: "musical"}} -> Broadway.Message.put_batcher(message, :musical)
        message -> message
      end
    else
      Broadway.Message.failed(message, "bookings-closed")
    end
  end

  @impl true
  def handle_batch(_batcher, messages, batch_info, _context) do
    Logger.info(
      "[Bookings] #{inspect(self())} Batch #{batch_info.batcher} #{batch_info.batch_key}"
    )

    messages
    |> Tickets.insert_all_tickets()
    |> Enum.each(fn message ->
      channel = message.metadata.amqp_channel

      email_payload = "email,#{message.data.user.email}"
      AMQP.Basic.publish(channel, "", "notifications_queue", email_payload)

      sms_payload = "sms,#{message.data.user.email}"
      AMQP.Basic.publish(channel, "", "notifications_queue", sms_payload)
    end)

    messages
  end
end
