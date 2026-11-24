using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System.Text;

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

// Conexão = socket TCP com o broker; Channel = "canal lógico" dentro da conexão,
// por onde passam os comandos AMQP (declarar exchange, publicar, etc.).
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string exchangeName = "orders.topic.exchange";

// Declara o exchange do tipo TOPIC (idempotente: se já existir com os mesmos
// parâmetros, não faz nada). No topic, a routing key é composta por palavras
// separadas por ponto (ex.: "orders.created") e o consumer faz o bind usando
// padrões com curingas: '*' casa exatamente 1 palavra, '#' casa 0 ou mais.
await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Topic,
    durable: true,      // sobrevive a restart do broker
    autoDelete: false,  // não é apagado quando o último bind some
    arguments: null
);

// Simula vários eventos de domínios diferentes (orders, payments, inventory).
// Cada tupla é (routing key, nome do evento). A routing key segue o formato
// "<dominio>.<acao>" — é ela que o exchange usa para decidir para quais filas
// a mensagem vai, comparando com os padrões dos bindings.
var events = new[]
{
    ("orders.created", "OrderCreated"),
    ("orders.cancelled", "OrderCancelled"),
    ("orders.paid", "OrderPaid"),
    ("payments.failed", "PaymentFailed"),
    ("payments.completed", "PaymentCompleted"),
    ("inventory.reserved", "InventoryReserved")
};

foreach (var (routingKey, eventName) in events)
{
    var orderId = Guid.NewGuid();
    var message = $"{eventName} | OrderId: {orderId}";
    var body = Encoding.UTF8.GetBytes(message); // AMQP transporta bytes, não strings

    // Publica no exchange com a routing key do evento. O producer NÃO conhece
    // as filas: quem decide o destino é o exchange, com base nos bindings.
    // Se nenhum binding casar com a routing key, a mensagem é descartada.
    await channel.BasicPublishAsync(
        exchange: exchangeName,
        routingKey: routingKey,
        body: body);

    Console.WriteLine($"[Producer] Published: {message}");
}

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