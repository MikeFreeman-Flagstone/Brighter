using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Paramore.Brighter.MessagingGateway.AzureServiceBus.AzureServiceBusWrappers;
using Paramore.Brighter.MessagingGateway.AzureServiceBus.ClientProvider;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

public class AzureServiceBusMessageGatewayProvider
    : Reactor.IAmAMessageGatewayReactorProvider,
      Proactor.IAmAMessageGatewayProactorProvider
{
    private readonly IServiceBusClientProvider _clientProvider;
    private readonly AdministrationClientWrapper _administrationClient;
    private readonly HashSet<string> _createdTopics = new();

    public AzureServiceBusMessageGatewayProvider()
    {
        _clientProvider = ASBCreds.ASBClientProvider;
        _administrationClient = new AdministrationClientWrapper(_clientProvider);
    }

    public RoutingKey GetOrCreateRoutingKey([CallerMemberName] string? testName = null)
    {
        return new RoutingKey($"Topic{Uuid.New():N}");
    }

    public ChannelName GetOrCreateChannelName([CallerMemberName] string? testName = null)
    {
        return new ChannelName($"Queue{Uuid.New():N}");
    }

    public AzureServiceBusPublication CreatePublication(RoutingKey routingKey, OnMissingChannel makeChannels = OnMissingChannel.Create)
    {
        _createdTopics.Add(routingKey.Value);
        return new AzureServiceBusPublication
        {
            Topic = routingKey,
            MakeChannels = makeChannels
        };
    }

    public AzureServiceBusSubscription CreateSubscription(
        RoutingKey routingKey,
        ChannelName channelName,
        OnMissingChannel makeChannel,
        bool setupDeadLetterQueue = false)
    {
        var config = new AzureServiceBusSubscriptionConfiguration();
        if (setupDeadLetterQueue)
        {
            config.DeadLetteringOnMessageExpiration = true;
            config.MaxDeliveryCount = 3;
        }

        return new AzureServiceBusSubscription<ASBTestCommand>(
            subscriptionName: new SubscriptionName(channelName.Value),
            channelName: channelName,
            routingKey: routingKey,
            makeChannels: makeChannel,
            subscriptionConfiguration: config,
            requeueCount: setupDeadLetterQueue ? 3 : 1,
            messagePumpType: MessagePumpType.Proactor
        );
    }

    public IAmAMessageProducerSync CreateProducer(AzureServiceBusPublication publication)
    {
        var registry = new AzureServiceBusProducerRegistryFactory(_clientProvider, new[] { publication }).Create();
        return (IAmAMessageProducerSync)registry.LookupBy(publication.Topic!);
    }

    public async Task<IAmAMessageProducerAsync> CreateProducerAsync(AzureServiceBusPublication publication, CancellationToken cancellationToken = default)
    {
        var registry = new AzureServiceBusProducerRegistryFactory(_clientProvider, new[] { publication }).Create();
        return (IAmAMessageProducerAsync)registry.LookupBy(publication.Topic!);
    }

    public IAmAChannelSync CreateChannel(AzureServiceBusSubscription subscription)
    {
        var consumerFactory = new AzureServiceBusConsumerFactory(_clientProvider);
        var consumer = consumerFactory.Create(subscription);
        var retryConsumer = new RetryAzureServiceBusMessageConsumer(consumer, maxRetries: 10);

        return new Channel(
            subscription.ChannelName,
            subscription.RoutingKey,
            retryConsumer,
            subscription.BufferSize);
    }

    public async Task<IAmAChannelAsync> CreateChannelAsync(AzureServiceBusSubscription subscription, CancellationToken cancellationToken = default)
    {
        var consumerFactory = new AzureServiceBusConsumerFactory(_clientProvider);
        var consumer = consumerFactory.Create(subscription);
        var retryConsumer = new RetryAzureServiceBusMessageConsumer(consumer, maxRetries: 10);

        return new ChannelAsync(
            subscription.ChannelName,
            subscription.RoutingKey,
            retryConsumer,
            subscription.BufferSize);
    }

    public void CleanUp(IAmAMessageProducerSync? producer, IAmAChannelSync? channel, IEnumerable<Message> messages)
    {
        channel?.Dispose();
        producer?.Dispose();

        foreach (var topic in _createdTopics)
        {
            _administrationClient.DeleteTopicAsync(topic).GetAwaiter().GetResult();
        }
    }

    public async Task CleanUpAsync(IAmAMessageProducerAsync? producer, IAmAChannelAsync? channel, IEnumerable<Message> messages)
    {
        if (channel != null)
        {
            await channel.PurgeAsync();
            channel.Dispose();
        }

        if (producer != null)
        {
            await producer.DisposeAsync();
        }

        foreach (var topic in _createdTopics)
        {
            await _administrationClient.DeleteTopicAsync(topic);
        }
    }

    public Message GetMessageFromDeadLetterQueue(AzureServiceBusSubscription subscription)
    {
        var client = _clientProvider.GetServiceBusClient();
        var receiver = client.CreateReceiver(
            subscription.RoutingKey.Value,
            subscription.ChannelName.Value,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        try
        {
            for (var i = 0; i < 10; i++)
            {
                var message = receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)).GetAwaiter().GetResult();
                if (message != null)
                {
                    var wrapper = new BrokeredMessageWrapper(message);
                    var creator = new AzureServiceBusMesssageCreator(subscription);
                    return creator.MapToBrighterMessage(wrapper);
                }

                Thread.Sleep(1000);
            }

            return new Message();
        }
        finally
        {
            receiver.DisposeAsync().GetAwaiter().GetResult();
        }
    }

    public async Task<Message> GetMessageFromDeadLetterQueueAsync(AzureServiceBusSubscription subscription, CancellationToken cancellationToken = default)
    {
        var client = _clientProvider.GetServiceBusClient();
        var receiver = client.CreateReceiver(
            subscription.RoutingKey.Value,
            subscription.ChannelName.Value,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        try
        {
            for (var i = 0; i < 10; i++)
            {
                var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (message != null)
                {
                    var wrapper = new BrokeredMessageWrapper(message);
                    var creator = new AzureServiceBusMesssageCreator(subscription);
                    return creator.MapToBrighterMessage(wrapper);
                }

                await Task.Delay(1000, cancellationToken);
            }

            return new Message();
        }
        finally
        {
            await receiver.DisposeAsync();
        }
    }

    private class BrokeredMessageWrapper : IBrokeredMessageWrapper
    {
        private readonly ServiceBusReceivedMessage _message;

        public BrokeredMessageWrapper(ServiceBusReceivedMessage message)
        {
            _message = message;
        }

        public byte[]? MessageBodyValue => _message.Body.ToArray();
        public IReadOnlyDictionary<string, object> ApplicationProperties => _message.ApplicationProperties;
        public string LockToken => _message.LockToken;
        public string Id => _message.MessageId;
        public string CorrelationId => string.IsNullOrEmpty(_message.CorrelationId) ? string.Empty : _message.CorrelationId;
        public string ContentType => _message.ContentType ?? string.Empty;
    }
}
