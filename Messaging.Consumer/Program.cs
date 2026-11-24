using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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

// Uma única conexão TCP com o broker; cada consumer terá seu próprio channel.
using var connection = await factory.CreateConnectionAsync();

// Mesmo nome do exchange declarado pelo Producer — é por ele que os dois se "encontram".
const string exchangeName = "orders.topic.exchange";

// Cada tupla é (nome da fila, padrão de binding). No topic exchange o binding
// usa curingas sobre as palavras da routing key (separadas por ponto):
//   '*' casa exatamente 1 palavra  → "payments.*" casa "payments.failed", mas não "payments.card.failed"
//   '#' casa 0 ou mais palavras    → "orders.#"  casa "orders", "orders.created", "orders.eu.created"...
// Uma mesma mensagem pode casar com vários bindings: ela é COPIADA para cada fila
// que casar (ex.: "orders.created" cai em "orders.all" E em "orders.created").
var subscriptions = new[]
{
    ("orders.all", "orders.#"),         // todos os eventos de orders
    ("orders.created", "orders.created"), // só o evento exato (sem curinga, vira match literal)
    ("payments.all", "payments.*"),     // eventos de payments com 1 palavra após o ponto
    ("inventory.all", "inventory.#")    // todos os eventos de inventory
};

// Manter os Channels vivos enquanto os Consumers estiverem ativos.
var channels = new List<IChannel>();

foreach (var (queueName, routingKey) in subscriptions)
{
    var channel = await connection.CreateChannelAsync();
    channels.Add(channel);

    // Declara o exchange topic com os MESMOS parâmetros do Producer (idempotente).
    // Declarar nos dois lados garante que funciona independente de quem sobe primeiro.
    await channel.ExchangeDeclareAsync(
        exchange: exchangeName,
        type: ExchangeType.Topic,
        durable: true,      // sobrevive a restart do broker
        autoDelete: false,  // não some quando o último consumer desconecta
        arguments: null);

    // Cria a fila desta assinatura (também idempotente).
    await channel.QueueDeclareAsync(
        queue: queueName,
        durable: true,     // fila sobrevive a restart do broker
        exclusive: false,  // outras conexões também podem usar a fila
        autoDelete: false, // fila não é apagada quando o consumer sai
        arguments: null);

    // O binding é onde a mágica do topic acontece: liga a fila ao exchange
    // com um PADRÃO. O exchange compara a routing key de cada mensagem
    // publicada com esse padrão para decidir se entrega uma cópia à fila.
    await channel.QueueBindAsync(
        queue: queueName,
        exchange: exchangeName,
        routingKey: routingKey);

    var consumer = new AsyncEventingBasicConsumer(channel);

    // Callback disparado para cada mensagem entregue pela fila.
    consumer.ReceivedAsync += (sender, eventArgs) =>
    {
        var body = eventArgs.Body.ToArray(); // payload chega como bytes
        var message = Encoding.UTF8.GetString(body);

        // eventArgs.RoutingKey é a key ORIGINAL da publicação (ex.: "orders.created"),
        // útil para ver qual padrão casou com o quê.
        Console.WriteLine($"[{queueName}] Received: {eventArgs.RoutingKey} | {message}");

        return Task.CompletedTask;
    };

    // Inicia o consumo. autoAck: true = o broker considera a mensagem entregue
    // assim que a envia (simples para o lab; em produção prefira ack manual,
    // senão mensagens se perdem se o consumer falhar no meio do processamento).
    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: true,
        consumer: consumer
    );

    Console.WriteLine($"Listening: {queueName} | Binding: {routingKey}");
}

// ReadLine segura o processo vivo; os consumers trabalham em background.
Console.WriteLine("All topic consumers running. Press ENTER to exit.");
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