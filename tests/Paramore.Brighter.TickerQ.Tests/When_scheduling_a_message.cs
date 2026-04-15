using System;
using System.Text.Json;
using Paramore.Brighter.TickerQ.Tests.TestDoubles;
using Paramore.Brighter.TickerQ.Tests.TestDoubles.Fixtures;


namespace Paramore.Brighter.TickerQ.Tests;

    public class TickerQSchedulerMessageTests : IDisposable
{
    private readonly TickerQMessageTestFixture _fixture;


    public TickerQSchedulerMessageTests()
    {
        _fixture = new TickerQMessageTestFixture();
    }

    [Test]
    public async Task When_scheduler_a_message_with_a_datetimeoffset_sync()
    {
        Message message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(1)));

        await Assert.That(id.Length).IsNotEqualTo(0);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsEmpty();

        Thread.Sleep(TimeSpan.FromSeconds(2));

        await Assert.That(_fixture.Outbox.Get(message.Id, new RequestContext())).IsEquivalentTo(message);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
    }

    [Test]
    public async Task When_scheduler_a_message_with_a_timespan_sync()
    {
        Message message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, TimeSpan.FromSeconds(1));

        await Assert.That(id.Length).IsNotEqualTo(0);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsEmpty();

        Thread.Sleep(TimeSpan.FromSeconds(2));

        await Assert.That(_fixture.Outbox.Get(message.Id, new RequestContext())).IsEquivalentTo(message);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
    }

    [Test]
    public async Task When_reschedule_a_message_with_a_datetimeoffset_sync()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(2)));

        await Assert.That((id)?.Any()).IsTrue();
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        scheduler.ReScheduler(id, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(5)));

        Thread.Sleep(TimeSpan.FromSeconds(2));
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        Thread.Sleep(TimeSpan.FromSeconds(5));

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
        await Assert.That(_fixture.Outbox.Get(message.Id, new RequestContext())).IsEquivalentTo(message);
    }

    [Test]
    public async Task When_reschedule_a_message_with_a_timespan_sync()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, TimeSpan.FromSeconds(2));

        await Assert.That((id)?.Any()).IsTrue();
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        scheduler.ReScheduler(id, TimeSpan.FromSeconds(5));

        Thread.Sleep(TimeSpan.FromSeconds(2));
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        await Task.Delay(TimeSpan.FromSeconds(5));

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
        await Assert.That(_fixture.Outbox.Get(message.Id, new RequestContext())).IsEquivalentTo(message);
    }
    [Test]
    public async Task When_cancel_scheduler_message_with_a_datetimeoffset()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, TimeSpan.FromSeconds(2));

        await Assert.That(id.Length).IsNotEqualTo(0);

        scheduler.Cancel(id);

        Thread.Sleep(TimeSpan.FromSeconds(3));

        var expected = Message.Empty;
        var actual = _fixture.Outbox.Get(message.Id, new RequestContext());

        await Assert.That(actual.Body).IsEquivalentTo(expected.Body);
        await Assert.That(actual.Id).IsEqualTo(expected.Id);
        await Assert.That(actual.Persist).IsEqualTo(expected.Persist);
        await Assert.That(actual.Redelivered).IsEqualTo(expected.Redelivered);
        await Assert.That(actual.DeliveryTag).IsEqualTo(expected.DeliveryTag);
        await Assert.That(actual.Header.MessageType).IsEqualTo(expected.Header.MessageType);
        await Assert.That(actual.Header.Topic).IsEqualTo(expected.Header.Topic);
        await Assert.That(actual.Header.TimeStamp).IsEqualTo(expected.Header.TimeStamp).Within(TimeSpan.FromSeconds(1));
        await Assert.That(actual.Header.CorrelationId).IsEqualTo(expected.Header.CorrelationId);
        await Assert.That(actual.Header.ReplyTo).IsEqualTo(expected.Header.ReplyTo);
        await Assert.That(actual.Header.ContentType).IsEqualTo(expected.Header.ContentType);
        await Assert.That(actual.Header.HandledCount).IsEqualTo(expected.Header.HandledCount);
    }


    [Test]
    public async Task When_cancel_scheduler_request_with_a_timespan()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerSync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = scheduler.Schedule(message, TimeSpan.FromSeconds(2));

        await Assert.That(id.Length).IsNotEqualTo(0);

        scheduler.Cancel(id);

        Thread.Sleep(TimeSpan.FromSeconds(3));

        var expected = Message.Empty;
        var actual = _fixture.Outbox.Get(message.Id, new RequestContext());

        await Assert.That(actual.Body).IsEquivalentTo(expected.Body);
        await Assert.That(actual.Id).IsEqualTo(expected.Id);
        await Assert.That(actual.Persist).IsEqualTo(expected.Persist);
        await Assert.That(actual.Redelivered).IsEqualTo(expected.Redelivered);
        await Assert.That(actual.DeliveryTag).IsEqualTo(expected.DeliveryTag);
        await Assert.That(actual.Header.MessageType).IsEqualTo(expected.Header.MessageType);
        await Assert.That(actual.Header.Topic).IsEqualTo(expected.Header.Topic);
        await Assert.That(actual.Header.TimeStamp).IsEqualTo(expected.Header.TimeStamp).Within(TimeSpan.FromSeconds(1));
        await Assert.That(actual.Header.CorrelationId).IsEqualTo(expected.Header.CorrelationId);
        await Assert.That(actual.Header.ReplyTo).IsEqualTo(expected.Header.ReplyTo);
        await Assert.That(actual.Header.ContentType).IsEqualTo(expected.Header.ContentType);
        await Assert.That(actual.Header.HandledCount).IsEqualTo(expected.Header.HandledCount);
    }

    private Message GetMessage()
    {
        var req = new MyEvent();
        var message =
            new Message(
                new MessageHeader { MessageId = req.Id, MessageType = MessageType.MT_EVENT, Topic = _fixture.RoutingKey },
                new MessageBody(JsonSerializer.Serialize(req)));
        return message;
    }

    public void Dispose()
    {
        _fixture.Clear();
    }
}

