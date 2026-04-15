using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Paramore.Brighter.Core.Tests.CommandProcessors.TestDoubles;
using Paramore.Brighter.Core.Tests.MessageDispatch.TestDoubles;
using Paramore.Brighter.Testing;
using Paramore.Brighter.ServiceActivator;

namespace Paramore.Brighter.Core.Tests.MessageDispatch.Proactor
{
    public class MessageDispatcherMultiplePerformerTestsAsync
    {
        private const string Topic = "myTopic";
        private const string ChannelName = "myChannel";
        private readonly Dispatcher _dispatcher;
        private readonly InternalBus _bus;
        public MessageDispatcherMultiplePerformerTestsAsync()
        {
            var routingKey = new RoutingKey(Topic);
            _bus = new InternalBus();
            var consumer = new InMemoryMessageConsumer(routingKey, _bus, TimeProvider.System, ackTimeout: TimeSpan.FromMilliseconds(1000));
            IAmAChannelSync channel = new Channel(new(ChannelName), new(Topic), consumer, 6);
            IAmACommandProcessor commandProcessor = new SpyCommandProcessor();
            var messageMapperRegistry = new MessageMapperRegistry(null, new SimpleMessageMapperFactoryAsync((_) => new MyEventMessageMapperAsync()));
            messageMapperRegistry.RegisterAsync<MyEvent, MyEventMessageMapperAsync>();
            var connection = new Subscription<MyEvent>(new SubscriptionName("test"), noOfPerformers: 3, timeOut: TimeSpan.FromMilliseconds(100), channelFactory: new InMemoryChannelFactory(_bus, TimeProvider.System), channelName: new ChannelName("fakeChannel"), messagePumpType: MessagePumpType.Proactor, routingKey: routingKey);
            _dispatcher = new Dispatcher(commandProcessor, new List<Subscription> { connection }, messageMapperRegistryAsync: messageMapperRegistry);
            var @event = new MyEvent();
            var message = new MyEventMessageMapperAsync().MapToMessageAsync(@event, new Publication { Topic = connection.RoutingKey }).GetAwaiter().GetResult();
            for (var i = 0; i < 6; i++)
                channel.Enqueue(message);
        }

        [Before(Test)]
        public async Task Setup()
        {
            await Assert.That(_dispatcher.State).IsEqualTo(DispatcherState.DS_AWAITING);
            _dispatcher.Receive();
        }

        [Test]
        public async Task WhenAMessageDispatcherStartsMultiplePerformers()
        {
            await Assert.That(_dispatcher.State).IsEqualTo(DispatcherState.DS_RUNNING);
            await Assert.That(_dispatcher.Consumers.Count()).IsEqualTo(3);
            await _dispatcher.End();
            await Assert.That(_bus.Stream(new RoutingKey(Topic))).IsEmpty();
            await Assert.That(_dispatcher.State).IsEqualTo(DispatcherState.DS_STOPPED);
        }
    }
}