
using Duracell;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using BuildingBlocks.RabbitMq;
using BuildingBlocks.Db;

namespace SomeBackgroundService;

public class OutboxService(ILogger<OutboxService> logger, IDbRepository dbRepository)
    : BackgroundService
{
    private readonly ILogger<OutboxService> _logger;
    private IConnection _connection;
    private IChannel _channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation("Rozpoczynanie usługi konsumowania RabbitMQ.");

            var factory = ConnectionProvider.Create();
            _connection = await factory.CreateConnectionAsync(stoppingToken);
            _channel = await _connection.CreateChannelAsync();

            await RabbitMqInitializer.InitializeAsync(_channel);

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += OnMessageReceive;
               
            await _channel.BasicConsumeAsync(queue: Stats.blob_requests_queue, autoAck: false, consumer: consumer);

            _logger.LogInformation("Konsument pomyślnie podłączony. Oczekiwanie na sygnał zatrzymania hosta...");
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Oczekiwane, gdy host jest zamykany
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Krytyczny błąd podczas łączenia/działania konsumenta RabbitMQ.");
        }
    }

    private async Task OnMessageReceive(object sender, BasicDeliverEventArgs eventArgs)
    {
        var body = eventArgs.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var props = eventArgs.BasicProperties;
        _logger.LogInformation("Otrzymano wiadomość: {Message}", message);


        var blob = dbRepository.InsertData(Encoding.UTF8.GetBytes("Jakiś blob do zapisania w bazie"));  //Zapisuje do tabeli i do outboxa!
        await _channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);                           //Po zapisie w bazie dopiero potwierdzam
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Zamykanie zasobów RabbitMQ (Channel i Connection).");
        await base.StopAsync(cancellationToken);

        _channel?.Dispose();
        _connection?.Dispose();
    }
}
