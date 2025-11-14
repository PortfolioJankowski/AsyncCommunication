
using BuildingBlocks.Db;
using BuildingBlocks.RabbitMq;
using RabbitMQ.Client;
using System.Text;

namespace SomeBackgroundService
{
    public class OutboxPublisherService(ILogger<OutboxPublisherService> logger, IServiceScopeFactory scopeFactory)
    : BackgroundService
    {
        private IConnection _connection;
        private IChannel _channel;

        private readonly TimeSpan PollingInterval = TimeSpan.FromSeconds(5);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Rozpoczynanie usługi publikowania Outbox.");

            var factory = ConnectionProvider.Create();
            _connection = await factory.CreateConnectionAsync(stoppingToken);
            _channel = await _connection.CreateChannelAsync();
            
            scopeFactory.CreateScope();
            var dbRepository = scopeFactory.CreateScope().ServiceProvider.GetRequiredService<IDbRepository>();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ProcessOutboxMessagesAsync(dbRepository);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Błąd podczas przetwarzania Outbox.");
                }

                await Task.Delay(PollingInterval, stoppingToken);
            }
        }

        private async Task ProcessOutboxMessagesAsync(IDbRepository dbRepository)
        {
            var blobIdToPublish = dbRepository.GetFromOutbox(); 
            if (blobIdToPublish == Guid.Empty)
            {
                logger.LogDebug("Brak nowych wiadomości Outbox do wysłania.");
                return;
            }

            var blobData = dbRepository.GetDataBlob(blobIdToPublish);
            if (blobData == null)
            {
                logger.LogWarning("Blob {Id} nie znaleziono, pomijam.", blobIdToPublish);
                return;
            }

            // 3. WYSYŁKA DO RABBITMQ
            var props = new BasicProperties
            {
                ContentType = "text/plain",
                DeliveryMode = DeliveryModes.Persistent,
                // KLUCZOWE: Użyjemy CorrelationId z tabeli Outbox, jeśli był zapisany, 
                // ale tutaj wysyłamy ID Bloba jako treść odpowiedzi:
                CorrelationId = dbRepository.GetCorrelationId(blobIdToPublish)
            };

            // Treść wiadomości to Id nowo utworzonego bloba
            var messageBody = Encoding.UTF8.GetBytes(blobIdToPublish.ToString());

            await _channel.BasicPublishAsync(
                exchange: Stats.exchange_name,
                routingKey: Stats.blob_responses_take_blob_key, // Używamy klucza dla odpowiedzi!
                mandatory: false,
                basicProperties: props,
                body: messageBody);

            logger.LogInformation("Opublikowano Id bloba {Id} do kolejki odpowiedzi.", blobIdToPublish);

             dbRepository.MarkOutboxAsProcessedAsync(blobIdToPublish);
        }
    }
}
