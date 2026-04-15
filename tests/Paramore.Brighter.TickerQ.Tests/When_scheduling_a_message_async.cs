using System;
using System.Text.Json;
using Paramore.Brighter.TickerQ.Tests.TestDoubles;
using Paramore.Brighter.TickerQ.Tests.TestDoubles.Fixtures;


namespace Paramore.Brighter.TickerQ.Tests;

[NotInParallel("Scheduler")]
[ClassDataSource<TickerQMessageTestFixture>(Shared = SharedType.PerClass)]
    public class TickerQSchedulerMessageAsyncTests : IDisposable
{
    private readonly TickerQMessageTestFixture _fixture;

    public TickerQSchedulerMessageAsyncTests(TickerQMessageTestFixture tickerQTestFixture)
    {
        _fixture = tickerQTestFixture;
    }

    [Test]
    public async Task When_scheduler_a_message_with_a_datetimeoffset_async()
    {
        Message message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message,
            _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(1)));

        await Assert.That(id.Length).IsNotEqualTo(0);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsEmpty();

        await Task.Delay(TimeSpan.FromSeconds(2));

        await Assert.That(await _fixture.Outbox.GetAsync(message.Id, new RequestContext())).IsEquivalentTo(message);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
    }

    [Test]
    public async Task When_scheduler_a_message_with_a_timespan_async()
    {
        Message message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message, TimeSpan.FromSeconds(4));

        await Assert.That(id.Length).IsNotEqualTo(0);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsEmpty();

        await Task.Delay(TimeSpan.FromSeconds(6));

        await Assert.That(await _fixture.Outbox.GetAsync(message.Id, new RequestContext())).IsEquivalentTo(message);

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
    }

    [Test]
    public async Task When_reschedule_a_message_with_a_datetimeoffset_async()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(2)));

        await Assert.That((id)?.Any()).IsTrue();
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();
        await scheduler.ReSchedulerAsync(id, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(5)));

        await Task.Delay(TimeSpan.FromSeconds(2));
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        await Task.Delay(TimeSpan.FromSeconds(5));

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
        await Assert.That(await _fixture.Outbox.GetAsync(message.Id, new RequestContext())).IsEquivalentTo(message);
    }

    [Test]
    public async Task When_reschedule_a_message_with_a_timespan_async()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(2)));

        await Assert.That((id)?.Any()).IsTrue();
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();
        var reScheduled = await scheduler.ReSchedulerAsync(id, TimeSpan.FromSeconds(5));
        await Assert.That(reScheduled).IsTrue();
        await Task.Delay(TimeSpan.FromSeconds(2));
        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

        await Task.Delay(TimeSpan.FromSeconds(6));

        await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
        await Assert.That(await _fixture.Outbox.GetAsync(message.Id, new RequestContext())).IsEquivalentTo(message);
    }

    [Test]
    public async Task When_cancel_scheduler_message_with_a_datetimeoffset_async()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message, TimeSpan.FromHours(1));

        await Assert.That((id)?.Any()).IsTrue();

        await scheduler.CancelAsync(id);

        await Task.Delay(TimeSpan.FromSeconds(2));

        var expected = Message.Empty;
        var actual = await _fixture.Outbox.GetAsync(message.Id, new RequestContext());

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
    public async Task When_cancel_scheduler_request_with_a_timespan_async()
    {
        var message = GetMessage();

        var scheduler = (IAmAMessageSchedulerAsync)_fixture.SchedulerFactory.Create(_fixture.Processor);
        var id = await scheduler.ScheduleAsync(message, TimeSpan.FromHours(1));

        await Assert.That((id)?.Any()).IsTrue();

        await scheduler.CancelAsync(id);

        await Task.Delay(TimeSpan.FromSeconds(2));

        var expected = Message.Empty;
        var actual = await _fixture.Outbox.GetAsync(message.Id, new RequestContext());

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

