using RabbitMQ.Client;
namespace BuildingBlocks.RabbitMq;
public static class ConnectionProvider
{
    public static ConnectionFactory Create()
    {
        return new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "guest",
            Port = 5672,
            Password = "guest",
            VirtualHost = "/",
        };
    }
}