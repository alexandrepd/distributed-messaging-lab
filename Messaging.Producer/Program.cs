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
// por onde passam os comandos AMQP (declarar fila, publicar, etc.).
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "orders.work";

// Work queue: aqui NÃO há exchange customizado. A fila é declarada diretamente
// e as mensagens são publicadas no exchange DEFAULT (""), um direct exchange
// implícito que roteia para a fila cujo nome é igual à routing key.
// A declaração é idempotente: se a fila já existir com os mesmos parâmetros, não faz nada.
await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,      // fila sobrevive a restart do broker
    exclusive: false,   // outras conexões também podem usar a fila
    autoDelete: false,  // não é apagada quando o último consumer sai
    arguments: null
);

// Publica 20 tarefas de uma vez, para dar trabalho aos workers.
for (var i = 1; i <= 20; i++)
{
    var message = $"Order: #{i}";
    var body = Encoding.UTF8.GetBytes(message); // AMQP transporta bytes, não strings

    // exchange: "" = exchange default; a routing key é o PRÓPRIO nome da fila.
    // Com vários consumers na MESMA fila, o broker distribui as mensagens em
    // round-robin: cada mensagem vai para UM único worker (competing consumers).
    await channel.BasicPublishAsync(
        exchange: "",
        routingKey: queueName,
        mandatory: false, // false = descarta em silêncio se a fila não existir
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