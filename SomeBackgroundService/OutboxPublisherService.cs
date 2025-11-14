
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
            var outboxRecord = dbRepository.GetFromOutbox(); 
            if (outboxRecord.Id == Guid.Empty)
            {
                logger.LogDebug("Brak nowych wiadomości Outbox do wysłania.");
                return;
            }

            var blobData = dbRepository.GetDataBlob(outboxRecord.Id);
            if (blobData == null)
            {
                logger.LogWarning("Blob {Id} nie znaleziono, pomijam.", outboxRecord.Id);
                return;
            }

            var props = new BasicProperties
            {
                ContentType = "text/plain",
                DeliveryMode = DeliveryModes.Persistent,
                CorrelationId = outboxRecord.CorrelationId.ToString(),
                ReplyTo = outboxRecord.ReplyTo
            };

            var messageBody = Encoding.UTF8.GetBytes(outboxRecord.Payload.ToString());

            await _channel.BasicPublishAsync(
                exchange: "", // jeśli chce wysłać bezpośrednio do kolejki Producenta (tej tymczasowej), to exchange jest puste a routingKey to nazwa tej kolejki
                routingKey: outboxRecord.ReplyTo,
                mandatory: false,
                basicProperties: props,
                body: messageBody);

            logger.LogInformation("Opublikowano Id bloba {Id} do kolejki odpowiedzi.", outboxRecord.Id);

             dbRepository.MarkOutboxAsProcessedAsync(outboxRecord.Id);
        }
    }
}
