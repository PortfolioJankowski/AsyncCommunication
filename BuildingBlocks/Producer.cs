using BuildingBlocks.RabbitMq;
using Duracell;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

/// <summary>
/// CEL : Wysyła żądanie do usługi konsumenta i oczekuje na odpowiedź przy użyciu wzorca RPC.
/// DZIAŁANIE:
/// 1. Inicjalizuje infrastrukturę RabbitMQ (kolejki, exchange itp.).
/// 2. Tworzy tymczasową, ekskluzywną kolejkę do odbioru odpowiedzi. Nazwę tej kolejki i correlationId ustawiamy przy publikowaniu wiadomości.
///     Potrzebuje tego, żeby mój publisher sam później konsumował odpowiedź.
/// 3. Konfiguruje konsumenta, który nasłuchuje na tej tymczasowej kolejce odpowiedzi.
///     Jeśli otrzyma wiadomość z pasującym correlationId, ustawia wynik TaskCompletionSource, co odblokowuje oczekujący Task.
/// 4. Publikuje wiadomość żądania do głównej kolejki z ustawionymi właściwościami ReplyTo i CorrelationId.
/// </summary>
public class Producer
{
    private readonly TaskCompletionSource<string> _tcs = new TaskCompletionSource<string>(); // Użyjemy TaskCompletionSource do asynchronicznego oczekiwania na odpowiedź
    private IChannel _channel;
    private string _replyQueueName;
    private string _correlationId;

    public async Task<string> Run(string blobData)
    {
        var connection = await ConnectionProvider.Create().CreateConnectionAsync();
        _channel = await connection.CreateChannelAsync();

        await RabbitMqInitializer.InitializeAsync(_channel);                            // 1. Inicjalizacja całej infrastruktury (wymaga IChannel)
       
        _replyQueueName = await _channel.QueueDeclareAsync(                             // 2. Utworzenie tymczasowej, ekskluzywnej kolejki na odpowiedź
            queue: "",                                                                  // Pusta nazwa, by RabbitMQ wygenerował unikalną
            durable: false,
            exclusive: true,                                                            // exclusive: true - kolejka zostanie usunięta po zamknięciu połączenia
            autoDelete: true);

        
        var consumer = new AsyncEventingBasicConsumer(_channel);                        // 3. Konfiguracja Consumer'a do słuchania odpowiedzi na tej tymczasowej kolejce
        consumer.ReceivedAsync += OnResponseReceived;
        await _channel.BasicConsumeAsync(queue: _replyQueueName, autoAck: true, consumer: consumer);

       
        _correlationId = Guid.NewGuid().ToString();                                      // 4. Konfiguracja właściwości wiadomości (ReplyTo i CorrelationId)

        var props = new BasicProperties
        {
            ContentType = "text/plain",
            DeliveryMode = DeliveryModes.Persistent,
            Expiration = "500000",
            ReplyTo = _replyQueueName,                                                   // WAŻNE: Gdzie wysłać odpowiedź
            CorrelationId = _correlationId                                              // WAŻNE: Identyfikator korelacji
        };

        
        await _channel.BasicPublishAsync(                                               // 5. Wysyłka żądania
            exchange: Stats.exchange_name,
            routingKey: Stats.blob_requests_take_blob_key,
            mandatory: false,
            basicProperties: props,
            body: Encoding.UTF8.GetBytes(blobData));

        Console.WriteLine($"Wysłano żądanie: '{blobData}' z CorrelationId: {_correlationId}");

       
        return await _tcs.Task;                                                         // 6. Czekanie na odpowiedź   
    }

    private Task OnResponseReceived(object sender, BasicDeliverEventArgs eventArgs)
    {
        // Sprawdź, czy CorrelationId pasuje do oczekiwanego
        if (eventArgs.BasicProperties.CorrelationId == _correlationId)
        {
            var responseBody = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            Console.WriteLine($"Odebrano odpowiedź: {responseBody}");
            _tcs.TrySetResult(responseBody);
        }
        // Jeśli CorrelationId nie pasuje, ignorujemy wiadomość (jest dla innego klienta)

        return Task.CompletedTask;
    }
}


//Czym Jest TaskCompletionSource?
//TaskCompletionSource to producent obiektu Task. Zwykły Task jest tworzony przez kompilator lub Task.Run i sam zarządza swoim stanem. Natomiast TCS pozwala Ci ręcznie kontrolować cykl życia i wynik powiązanego z nim Taska.

//Kluczowe Elementy
//TaskCompletionSource<TResult> tcs = new TaskCompletionSource<TResult>(): Tworzysz obiekt TCS.

//Task t = tcs.Task: To jest kluczowa właściwość. Generuje ona obiekt Task, na którym możesz wywołać await. Ten Task jest początkowo w stanie oczekiwania (WaitingForActivation).

//Metody Sterujące (Ustawienie Wyniku/Stanów): W momencie, gdy asynchroniczna operacja się zakończy (np. RabbitMQ dostarczy wiadomość), wywołujesz jedną z metod na obiekcie tcs:

//tcs.SetResult(TResult result): Ustawia stan Task jako ukończony i przekazuje wynik.

//tcs.SetException(Exception ex) / tcs.SetException(IEnumerable<Exception> exceptions): Ustawia stan Task jako błąd (Faulted). await na tym Task spowoduje rzucenie wyjątku.

//tcs.SetCanceled(): Ustawia stan Task jako anulowany (Canceled).