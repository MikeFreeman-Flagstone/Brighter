using System;
using System.Linq;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway.Reactor;

[NotInParallel("Redis Shared Pool")]   //shared connection pool so run sequentially
[Category("Redis")]
[ClassDataSource<RedisFixture>(Shared = SharedType.PerClass)]
    public class RedisRequeueMessageTests 
{
    private readonly RedisFixture _redisFixture;
    private readonly Message _messageOne;
    private readonly Message _messageTwo;

    public RedisRequeueMessageTests(RedisFixture redisFixture)
    {
        _redisFixture = redisFixture;
        var routingKey = redisFixture.Topic;

        _messageOne = new Message(
            new MessageHeader(Guid.NewGuid().ToString(), routingKey, MessageType.MT_COMMAND),
            new MessageBody("test content")
        );

        _messageTwo = new Message(
            new MessageHeader(Guid.NewGuid().ToString(), routingKey, MessageType.MT_COMMAND),
            new MessageBody("more test content")
        );
    }

    [Test]
    public async Task When_requeing_a_failed_message()
    {
        //Need to receive to subscribe to feed, before we send a message. This returns an empty message we discard
        _redisFixture.MessageConsumer.Receive(TimeSpan.FromMilliseconds(1000));

        //Send a sequence of messages, we want to check that ordering is preserved
        _redisFixture.MessageProducer.Send(_messageOne);
        _redisFixture.MessageProducer.Send(_messageTwo);

        //Now receive, the first message
        var sentMessageOne = _redisFixture.MessageConsumer.Receive(TimeSpan.FromMilliseconds(1000)).Single();

        //now requeue the first message
        _redisFixture.MessageConsumer.Requeue(_messageOne);

        //try receiving again; messageTwo should come first
        var sentMessageTwo = _redisFixture.MessageConsumer.Receive(TimeSpan.FromMilliseconds(1000)).Single();
        var messageBodyTwo = sentMessageTwo.Body.Value;
        _redisFixture.MessageConsumer.Acknowledge(sentMessageTwo);

        sentMessageOne = _redisFixture.MessageConsumer.Receive(TimeSpan.FromMilliseconds(1000)).Single();
        var messageBodyOne = sentMessageOne.Body.Value;
        _redisFixture.MessageConsumer.Acknowledge(sentMessageOne);

        await Assert.That(messageBodyOne).IsEqualTo(_messageOne.Body.Value);
        await Assert.That(messageBodyTwo).IsEqualTo(_messageTwo.Body.Value);
    }
}
