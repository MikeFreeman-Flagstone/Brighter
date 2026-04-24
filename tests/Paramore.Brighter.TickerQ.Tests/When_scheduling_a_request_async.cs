using System;
using Paramore.Brighter.TickerQ.Tests.TestDoubles;
using Paramore.Brighter.TickerQ.Tests.TestDoubles.Fixtures;


namespace Paramore.Brighter.TickerQ.Tests
{
    // TickerQ uses a process-global TickerFunctionProvider; running tests in parallel causes cross-fixture job dispatch.
    [NotInParallel("TickerQScheduler")]
    public class TickerQSchedulerRequestAsyncTests : IDisposable
    {
        private readonly TickerQRequestAsyncTestFixture _fixture;

        public TickerQSchedulerRequestAsyncTests()
        {
            _fixture = new TickerQRequestAsyncTestFixture();
        }

        #region Scheduler

        [Test]
        public async Task When_scheduler_send_request_with_a_datetimeoffset_async()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Send,
                _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(1)));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        public async Task When_scheduler_send_request_with_a_timespan_asc()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Send, TimeSpan.FromSeconds(1));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        public async Task When_scheduler_publish_request_with_a_datetimeoffset_async()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Publish,
                _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(1)));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        public async Task When_scheduler_publish_request_with_a_timespan()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Publish, TimeSpan.FromSeconds(1));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        public async Task When_scheduler_post_request_with_a_datetimeoffset_async()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Post,
                _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(1)));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(await _fixture.Outbox.GetAsync(req.Id, new RequestContext())).IsNotEqualTo(Message.Empty);

            await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();
        }

        [Test]
        public async Task When_scheduler_post_request_with_a_timespan_async()
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, RequestSchedulerType.Post, TimeSpan.FromSeconds(1));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey) ?? []).IsEmpty();

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Assert.That(_fixture.InternalBus.Stream(_fixture.RoutingKey)).IsNotEmpty();

            await Assert.That(await _fixture.Outbox.GetAsync(req.Id, new RequestContext())).IsNotEqualTo(Message.Empty);
        }

        #endregion

        #region Rescheduler

        [Test]
        [Arguments(RequestSchedulerType.Send)]
        [Arguments(RequestSchedulerType.Publish)]
        public async Task When_reschedule_request_with_a_datetimeoffset_async(RequestSchedulerType type)
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, type, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(2)));
        
            await Assert.That((id)?.Any()).IsTrue();

            await scheduler.ReSchedulerAsync(id, _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(5)));

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(2));
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(5));
            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        [Arguments(RequestSchedulerType.Send)]
        [Arguments(RequestSchedulerType.Publish)]
        public async Task When_reschedule_send_request_with_a_timespan_async(RequestSchedulerType type)
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, type, TimeSpan.FromSeconds(2));

            await Assert.That((id)?.Any()).IsTrue();
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await scheduler.ReSchedulerAsync(id, TimeSpan.FromSeconds(5));

            await Task.Delay(TimeSpan.FromSeconds(2));
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await Task.Delay(TimeSpan.FromSeconds(5));
            await Assert.That(_fixture.ReceivedMessages).ContainsKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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

        #endregion

        #region Cancel

        [Test]
        [Arguments(RequestSchedulerType.Send)]
        [Arguments(RequestSchedulerType.Post)]
        [Arguments(RequestSchedulerType.Publish)]
        public async Task When_cancel_scheduler_request_with_a_datetimeoffset(RequestSchedulerType type)
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, type,
                _fixture.TimeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(2)));

            await Assert.That((id)?.Any()).IsTrue();

            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await scheduler.CancelAsync(id);

            await Task.Delay(TimeSpan.FromSeconds(3));
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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
        [Arguments(RequestSchedulerType.Send)]
        [Arguments(RequestSchedulerType.Post)]
        [Arguments(RequestSchedulerType.Publish)]
        public async Task When_cancel_scheduler_request_with_a_timespan_async(RequestSchedulerType type)
        {
            var req = new MyEvent();
            var scheduler = _fixture.SchedulerFactory.CreateAsync(_fixture.Processor);
            var id = await scheduler.ScheduleAsync(req, type, TimeSpan.FromSeconds(2));

            await Assert.That((id)?.Any()).IsTrue();
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            await scheduler.CancelAsync(id);

            await Task.Delay(TimeSpan.FromSeconds(3));
            await Assert.That(_fixture.ReceivedMessages).DoesNotContainKey(nameof(MyEventHandlerAsync));

            var expected = Message.Empty;
            var actual = await _fixture.Outbox.GetAsync(req.Id, new RequestContext());

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

        #endregion

        public void Dispose()
        {
            _fixture.Clear();
        }
    }
}
