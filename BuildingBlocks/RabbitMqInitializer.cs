using RabbitMQ.Client;

namespace Duracell
{
    /// <summary>
    /// Inicjalizuje pełną infrastrukturę RabbitMQ dla systemu żądania/odpowiedzi (Request/Reply) bloba 
    /// z kaskadowym obsługiwaniem wiadomości martwych (Dead Lettering).
    /// 
    /// 1. Konfiguruje główny Exchange (amq.direct) i Kolejki:
    ///    - 'blob_requests_queue' (żądania, powiązane z DLX).
    ///    - 'blob_responses_queue' (odpowiedzi).
    /// 
    /// 2. Ustanawia kaskadę Dead Letter Queue (DLQ):
    ///    - 'blob_requests_queue' kieruje martwe wiadomości do 'dlx_exchange'.
    ///    - 'dlx_queue' (DLQ) jest powiązana z 'dlx_exchange' i ma TTL ustawiony na 24 godziny.
    ///    - Po wygaśnięciu TTL, 'dlx_queue' kieruje wiadomość do 'dead_end_exchange'.
    ///    - 'dead_end_point_queue' (Finalny Dead End) jest powiązana z 'dead_end_exchange', 
    ///      służąc jako ostateczne archiwum nieprzetworzonych wiadomości.
    /// </summary>
    public static class RabbitMqInitializer
    {
        public static async Task InitializeAsync(IChannel channel)
        {
            await channel.ExchangeDeclareAsync(
                exchange: Stats.exchange_name,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false);

            var blobRequestsQueueArgs = new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", Stats.dead_letter_exchange_name },
                { "x-dead-letter-routing-key", Stats.dead_letter_routing_key }
            };

            await channel.QueueDeclareAsync(
                queue: Stats.blob_requests_queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: blobRequestsQueueArgs);

            await channel.QueueBindAsync(
                queue: Stats.blob_requests_queue,
                exchange: Stats.exchange_name,
                routingKey: Stats.blob_requests_take_blob_key);

            await channel.QueueDeclareAsync(
                queue: Stats.blob_responses_queue,
                durable: true,
                exclusive: false,
                autoDelete: false);

            await channel.QueueBindAsync(
                queue: Stats.blob_responses_queue,
                exchange: Stats.exchange_name,
                routingKey: Stats.blob_responses_take_blob_key);

            await channel.ExchangeDeclareAsync(
                exchange: "dead_end_exchange", // Nowy Exchange
                type: ExchangeType.Direct,
                durable: true);

            await channel.QueueDeclareAsync(
                queue: Stats.dead_end_point_queue_name, // Kolejka Dead End
                durable: true,
                exclusive: false,
                autoDelete: false);

            await channel.ExchangeDeclareAsync(
               exchange: Stats.dead_letter_exchange_name,
               type: ExchangeType.Direct,
               durable: true,
               autoDelete: false);

            var dlqArgs = new Dictionary<string, object>
            {
                {"x-message-ttl", 86400000}, 
                // Konfigurujemy DLX dla dlx_queue_name (który będzie prowadził do Dead End)
                {"x-dead-letter-exchange", "dead_end_exchange"},
                {"x-dead-letter-routing-key", "dead_end_key"}
            };

            await channel.QueueDeclareAsync(
                queue: Stats.dead_letter_queue_name,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: dlqArgs);

            await channel.QueueBindAsync(
                queue: Stats.dead_letter_queue_name,
                exchange: Stats.dead_letter_exchange_name,
                routingKey: Stats.dead_letter_routing_key);


            await channel.QueueBindAsync(
                queue: Stats.dead_end_point_queue_name,
                exchange: "dead_end_exchange",
                routingKey: "dead_end_key"); // Nowy klucz routingu
        }
    }
}
