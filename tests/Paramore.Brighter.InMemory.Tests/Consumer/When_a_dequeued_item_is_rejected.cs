using System;
using System.Linq;
using Microsoft.Extensions.Time.Testing;

namespace Paramore.Brighter.InMemory.Tests.Consumer;

public class InMemoryConsumerRejectTests
{
    [Test]
    public async Task When_a_dequeued_item_is_rejected()
    {
        //arrange
        const string myTopic = "my topic";
        var routingKey = new RoutingKey(myTopic);

        var expectedMessage = new Message(
            new MessageHeader(Id.Random(), routingKey, MessageType.MT_EVENT),
            new MessageBody("a test body"));
        
        var bus = new InternalBus();
        bus.Enqueue(expectedMessage);

        var timeProvider = new FakeTimeProvider();
        var consumer = new InMemoryMessageConsumer(routingKey, bus, timeProvider, ackTimeout: TimeSpan.FromMilliseconds(1000));
        
        //act
        var receivedMessage = consumer.Receive().Single();
        consumer.Reject(receivedMessage);
        
        timeProvider.Advance(TimeSpan.FromSeconds(2));  //-- the message should be returned to the bus if there is no Acknowledge or Reject
        
        //assert
        await Assert.That(bus.Stream(routingKey)).IsEmpty();
    }
}
