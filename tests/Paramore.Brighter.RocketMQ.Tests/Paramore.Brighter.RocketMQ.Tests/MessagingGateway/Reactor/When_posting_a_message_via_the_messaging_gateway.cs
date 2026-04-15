using System.Net.Mime;
using System.Text.Json;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.RocketMQ.Tests.TestDoubles;
using Paramore.Brighter.RocketMQ.Tests.Utils;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway.Reactor;

[Category("RocketMQ")]
public class MessageProducerSendTests  : IDisposable 
{
    private readonly Message _message;
    private readonly IAmAChannelSync _channel;
    private readonly IAmAMessageProducerSync _messageProducer;
    private readonly MyCommand _myCommand;
    private readonly Id _correlationId;
    private readonly RoutingKey _replyTo;

    public MessageProducerSendTests()
    {
        _myCommand = new MyCommand { Value = "Test" };
        _correlationId = Id.Random();
        _replyTo = new RoutingKey("http:\\queueUrl");
        ContentType contentType = new(MediaTypeNames.Text.Plain);
        var channelName = Guid.NewGuid().ToString();
        var publication = new RocketMqPublication{ Topic = "rmq_post_via_gateway" };

        RocketMqSubscription<MyCommand> mqSubscription = new(
            subscriptionName: new SubscriptionName(channelName),
            channelName: new ChannelName(channelName),
            routingKey: publication.Topic!,
            consumerGroup: Guid.NewGuid().ToString(),
            messagePumpType: MessagePumpType.Proactor
        );

        _message = new Message(
            new MessageHeader(_myCommand.Id, publication.Topic!, MessageType.MT_COMMAND, correlationId: _correlationId,
                replyTo: new RoutingKey(_replyTo), contentType: contentType),
            new MessageBody(JsonSerializer.Serialize((object)_myCommand, JsonSerialisationOptions.Options))
        );

        var connection = GatewayFactory.CreateConnection();
        var channelFactory = new RocketMqChannelFactory(new RocketMessageConsumerFactory(connection));
        _channel = channelFactory.CreateSyncChannel(mqSubscription);
        _messageProducer = new RocketMqMessageProducer(
            connection, 
            GatewayFactory.CreateProducer(connection, publication).GetAwaiter().GetResult(),
            publication);
    }

    [Test]
    public async Task When_posting_a_message_via_the_producer_async()
    {
        _channel.Purge();
        
        // arrange
        _message.Header.Subject = "test subject";
        _messageProducer.Send(_message);
        
        Thread.Sleep(1000);

        var message = _channel.Receive(TimeSpan.FromMilliseconds(5000));

        // clear the queue
        _channel.Acknowledge(message);

        // should_send_the_message_to_aws_sqs
        await Assert.That(message.Header.MessageType).IsEqualTo(MessageType.MT_COMMAND);

        await Assert.That(message.Id).IsEqualTo(_myCommand.Id);
        await Assert.That(message.Redelivered).IsFalse();
        await Assert.That(message.Header.MessageId).IsEqualTo(_myCommand.Id);
        await Assert.That(message.Header.Topic.Value).Contains(_messageProducer.Publication.Topic!.Value);
        await Assert.That(message.Header.CorrelationId).IsEqualTo(_correlationId);
        await Assert.That(message.Header.ReplyTo).IsEqualTo(_replyTo);
        await Assert.That(message.Header.HandledCount).IsEqualTo(0);
        await Assert.That(message.Header.Subject).IsEqualTo(_message.Header.Subject);
        // allow for clock drift in the following test, more important to have a contemporary timestamp than anything
        await Assert.That((message.Header.TimeStamp) > (RoundToSeconds(DateTime.UtcNow.AddMinutes(-1)))).IsTrue();
        await Assert.That(message.Header.Delayed).IsEqualTo(TimeSpan.Zero);
        // {"Id":"cd581ced-c066-4322-aeaf-d40944de8edd","Value":"Test","WasCancelled":false,"TaskCompleted":false}
        await Assert.That(message.Body.Value).IsEqualTo(_message.Body.Value);
    }
    
    public void Dispose()
    {
        _messageProducer.Dispose();
    }

    private static DateTime RoundToSeconds(DateTime dateTime)
    {
        return new DateTime(dateTime.Ticks - (dateTime.Ticks % TimeSpan.TicksPerSecond), dateTime.Kind);
    }
}
