using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});
var configuration = builder.Configuration;

var factory = new ConnectionFactory
{
    HostName = GetRequiredSetting(configuration, "RabbitMq:Host"),
    Port = int.Parse(GetRequiredSetting(configuration, "RabbitMq:Port")),
    UserName = GetRequiredSetting(configuration, "RabbitMq:UserName"),
    Password = GetRequiredSetting(configuration, "RabbitMq:Password")
};

using var connection = await factory.CreateConnectionAsync();
const string exchangeName = "orders.fanout.exchange";

var services = new[]
{
    // "payment",
    "inventory",
    "notification"
};

// Manter os Channels vivos enquanto os Consumers estiverem ativos.
var channels = new List<IChannel>();

foreach (var service in services)
{
    var channel = await connection.CreateChannelAsync();
    channels.Add(channel);

    //declarar exchange fanout y cola para cada servicio
    await channel.ExchangeDeclareAsync(
        exchange: exchangeName,
        type: ExchangeType.Fanout,
        durable: true,
        autoDelete: false,
        arguments: null);

    var queueName = $"Ordes.{service}";

    //criar fila para o serviço
    await channel.QueueDeclareAsync(
        queue: queueName,
        durable: true,
        exclusive: false,
        autoDelete: false,
        arguments: null);

    //vincular fila ao exchange fanout
    await channel.QueueBindAsync(
        queue: queueName,
        exchange: exchangeName,
        routingKey: "");

    var consumer = new AsyncEventingBasicConsumer(channel);

    //tratar mensagens recebidas do exchange fanout
    consumer.ReceivedAsync += (sender, eventArgs) =>
    {
        var body = eventArgs.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        Console.WriteLine($"[{service.ToUpper()}] received: {message}");

        return Task.CompletedTask;
    };

    //iniciar consumo da fila do serviço
    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: true,
        consumer: consumer
    );

    Console.WriteLine($"Consumer for service '{service}' is running.");
}

Console.WriteLine("All consumers running. Press ENTER to exit.");
Console.ReadLine();

static string GetRequiredSetting(IConfiguration configuration, string key)
{
    var value = configuration[key];
    if (string.IsNullOrWhiteSpace(value))
    {
        throw new InvalidOperationException($"Required configuration '{key}' is missing.");
    }

    if (value == "__SET_WITH_USER_SECRETS__")
    {
        throw new InvalidOperationException($"Configuration '{key}' is still a placeholder; set it with dotnet user-secrets.");
    }

    return value;
}