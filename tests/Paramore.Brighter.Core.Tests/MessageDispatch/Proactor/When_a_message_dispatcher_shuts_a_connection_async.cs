using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Paramore.Brighter.Core.Tests.CommandProcessors.TestDoubles;
using Paramore.Brighter.Core.Tests.MessageDispatch.TestDoubles;
using Paramore.Brighter.Testing;
using Paramore.Brighter.ServiceActivator;

namespace Paramore.Brighter.Core.Tests.MessageDispatch.Proactor
{
    public class MessageDispatcherShutConnectionTests
    {
        private const string Topic = "fakekey";
        private const string ChannelName = "fakeChannel";
        private readonly Dispatcher _dispatcher;
        private readonly Subscription _subscription;
        private readonly RoutingKey _routingKey = new(Topic);
        private readonly FakeTimeProvider _timeProvider = new();
        public MessageDispatcherShutConnectionTests()
        {
            InternalBus bus = new();
            IAmACommandProcessor commandProcessor = new SpyCommandProcessor();
            var messageMapperRegistry = new MessageMapperRegistry(null, new SimpleMessageMapperFactoryAsync((_) => new MyEventMessageMapperAsync()));
            messageMapperRegistry.RegisterAsync<MyEvent, MyEventMessageMapperAsync>();
            _subscription = new Subscription<MyEvent>(new SubscriptionName("test"), noOfPerformers: 3, timeOut: TimeSpan.FromMilliseconds(1000), channelFactory: new InMemoryChannelFactory(bus, _timeProvider), channelName: new ChannelName(ChannelName), messagePumpType: MessagePumpType.Proactor, routingKey: _routingKey);
            _dispatcher = new Dispatcher(commandProcessor, new List<Subscription> { _subscription }, messageMapperRegistryAsync: messageMapperRegistry);
            var @event = new MyEvent();
            var message = new MyEventMessageMapperAsync().MapToMessageAsync(@event, new Publication { Topic = _subscription.RoutingKey }).GetAwaiter().GetResult();
            for (var i = 0; i < 6; i++)
                bus.Enqueue(message);
        }

        [Before(Test)]
        public async Task Setup()
        {
            await Assert.That(_dispatcher.State).IsEqualTo(DispatcherState.DS_AWAITING);
            _dispatcher.Receive();
        }

        [Test]
        public async Task When_A_Message_Dispatcher_Shuts_A_Connection()
        {
            await Task.Delay(1000);
            _dispatcher.Shut(_subscription);
            await _dispatcher.End();
            await Assert.That(_dispatcher.Consumers).DoesNotContain(consumer => consumer.Name == _subscription.Name && consumer.State == ConsumerState.Open);
            await Assert.That(_dispatcher.State).IsEqualTo(DispatcherState.DS_STOPPED);
            await Assert.That(_dispatcher.Consumers).IsEmpty();
        }

        [After(Test)]
        public void Dispose()
        {
            if (_dispatcher?.State == DispatcherState.DS_RUNNING)
                _dispatcher.End().Wait();
        }
    }
}