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

// Nome do worker: 1º argumento da linha de comando ou, se faltar, o PID do processo.
// Serve para distinguir as instâncias quando vários workers rodam em paralelo.
var consumerName = args.FirstOrDefault() ?? $"worker-{Environment.ProcessId}";

const string queueName = "orders.work";

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// Declara a MESMA fila do producer, com os mesmos parâmetros (idempotente) —
// assim funciona independente de quem sobe primeiro.
await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,     // fila sobrevive a restart do broker
    exclusive: false,  // outras conexões também podem usar a fila
    autoDelete: false, // fila não é apagada quando o consumer sai
    arguments: null);

var consumer = new AsyncEventingBasicConsumer(channel);

// Callback disparado para cada mensagem que o broker entrega a ESTE worker.
// Como todos os workers consomem a mesma fila, cada mensagem chega a um único worker.
consumer.ReceivedAsync += async (sender, eventArgs) =>
{
    var body = eventArgs.Body.ToArray(); // payload chega como bytes
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"[{consumerName}] Received: {message}");

    // Simula um processamento demorado (1s por mensagem). Rodando 2+ workers,
    // é isso que torna visível a distribuição round-robin entre eles.
    await Task.Delay(1000);

    Console.WriteLine($"[{consumerName}] Processed: {message}");
};

// Inicia o consumo. autoAck: true = o broker dá a mensagem como entregue assim
// que a envia; se o worker morrer no meio do Task.Delay, a mensagem se perde.
// Em produção: autoAck: false + BasicAckAsync após processar, e
// BasicQosAsync(prefetchCount: 1) para "fair dispatch" (o broker só manda a
// próxima mensagem quando o worker confirmar a atual).
await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: true,
    consumer: consumer
);

Console.WriteLine($"Listening on: {queueName}");

// ReadLine segura o processo vivo; o consumer trabalha em background.
Console.WriteLine($"Worker '{consumerName}' running. Press ENTER to exit.");
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