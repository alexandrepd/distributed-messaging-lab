using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

// Monta o host genérico do .NET só para carregar a configuração
// (appsettings.json, appsettings.Development.json, user-secrets, variáveis de ambiente).
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});
var configuration = builder.Configuration;

// Fábrica de conexões: aponta para o broker RabbitMQ usando os valores da configuração.
var factory = new ConnectionFactory
{
    HostName = GetRequiredSetting(configuration, "RabbitMq:Host"),
    Port = int.Parse(GetRequiredSetting(configuration, "RabbitMq:Port")),
    UserName = GetRequiredSetting(configuration, "RabbitMq:UserName"),
    Password = GetRequiredSetting(configuration, "RabbitMq:Password")
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "orders.work";


await channel.BasicQosAsync(
prefetchSize: 0,
prefetchCount: 1,
global: false
);

var consumer = new AsyncEventingBasicConsumer(channel);

// Callback disparado para cada mensagem que o broker entrega a ESTE worker.
// Como todos os workers consomem a mesma fila, cada mensagem chega a um único worker.
consumer.ReceivedAsync += async (sender, eventArgs) =>
{
    var body = eventArgs.Body.ToArray(); // payload chega como bytes
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received: {message}");

    try
    {
        Console.WriteLine($"Processing: {message}");
        await Task.Delay(1500);

        if (message.Contains("Order: #5"))
            throw new Exception($"Simulated processing error with message: [{message}]");

        await channel.BasicRejectAsync(
            deliveryTag: eventArgs.DeliveryTag,
            requeue: false
        );

        Console.WriteLine($"ACK: [{message}]");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error processing message: {ex.Message}");

        await channel.BasicNackAsync(
            deliveryTag: eventArgs.DeliveryTag,
            multiple: false,
            requeue: false
        );

        Console.WriteLine($"NACK: {message}");
    }
};

await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: false,
    consumer: consumer);

Console.WriteLine($"Consumer listening on {queueName}");
Console.WriteLine("Press ENTER to exit.");

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