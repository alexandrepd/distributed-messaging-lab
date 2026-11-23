# Distributed Messaging Lab

Projeto de exemplo com dois aplicativos .NET que trocam mensagens por uma fila RabbitMQ:

- **Messaging.Producer** publica uma mensagem de pedido na fila `orders`.
- **Messaging.Consumer** recebe e exibe as mensagens dessa fila.

## Requisitos

- .NET SDK 11
- Docker com Docker Compose

## Configuração local

O `appsettings.json` de cada aplicativo contém configurações não sensíveis, como host, porta e nome da fila. `appsettings.Development.json` mostra os campos de usuário e senha com placeholders; os valores reais devem ser armazenados localmente com [User Secrets](https://learn.microsoft.com/aspnet/core/security/app-secrets), que substituem esses placeholders no ambiente de desenvolvimento.

O Docker Compose lê as credenciais do RabbitMQ de um arquivo `.env` local, que não deve ser commitado:

```powershell
Copy-Item .env.example .env
notepad .env
```

Edite `.env` e defina um usuário e uma senha locais. Em seguida, grave os mesmos valores nos User Secrets dos dois projetos:

```powershell
dotnet user-secrets set "RabbitMq:UserName" "lab_user" --project Messaging.Consumer
dotnet user-secrets set "RabbitMq:Password" "sua-senha-local" --project Messaging.Consumer
dotnet user-secrets set "RabbitMq:UserName" "lab_user" --project Messaging.Producer
dotnet user-secrets set "RabbitMq:Password" "sua-senha-local" --project Messaging.Producer
```

Substitua `lab_user` e `sua-senha-local` pelos mesmos valores configurados em `.env`. User Secrets são para desenvolvimento local; não são criptografados e não devem ser usados como cofre de produção.

O Compose usa essas variáveis para inicializar o RabbitMQ somente quando o volume de dados está vazio. Se você já tiver um volume criado, alterar `.env` não troca as credenciais existentes; use as credenciais atuais ou altere o usuário no próprio RabbitMQ. `docker compose down` preserva os dados.

## Executar

1. Inicie o RabbitMQ:

   ```powershell
   docker compose up -d
   ```

2. Em um terminal, inicie o consumidor:

   ```powershell
   dotnet run --project Messaging.Consumer
   ```

3. Em outro terminal, publique uma mensagem:

   ```powershell
   dotnet run --project Messaging.Producer
   ```

O consumidor permanece ativo até pressionar Enter. O RabbitMQ usa a porta `5672`; sua interface de gerenciamento está em [http://localhost:15672](http://localhost:15672). Use o usuário e a senha definidos em `.env`.

Para parar o RabbitMQ, execute:

```powershell
docker compose down
```

## Estrutura

- `Messaging.Producer/`: aplicativo que publica mensagens.
- `Messaging.Consumer/`: aplicativo que consome mensagens.
- `docker-compose.yaml`: serviço RabbitMQ e armazenamento persistente.
- `DistributedMessagingLab.slnx`: solução .NET.
