using System;
using System.Threading;
using System.Threading.Tasks;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

/// <summary>
/// A decorator for Azure Service Bus message consumers that retries receiving a message
/// when the underlying consumer returns an empty result or MT_NONE message.
/// This is useful in CI environments where ASB may be slow to deliver messages.
/// </summary>
public class RetryAzureServiceBusMessageConsumer : IAmAMessageConsumerSync, IAmAMessageConsumerAsync
{
    private readonly IAmAMessageConsumerSync _innerSync;
    private readonly IAmAMessageConsumerAsync _innerAsync;
    private readonly int _maxRetries;

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryAzureServiceBusMessageConsumer"/> class.
    /// </summary>
    /// <param name="inner">The underlying Azure Service Bus message consumer to decorate.</param>
    /// <param name="maxRetries">The maximum number of receive attempts (minimum 1).</param>
    public RetryAzureServiceBusMessageConsumer(IAmAMessageConsumerSync inner, int maxRetries = 10)
    {
        _innerSync = inner ?? throw new ArgumentNullException(nameof(inner));
        _innerAsync = inner as IAmAMessageConsumerAsync ?? throw new ArgumentException("Inner consumer must implement IAmAMessageConsumerAsync", nameof(inner));
        _maxRetries = Math.Max(1, maxRetries);
    }

    public void Acknowledge(Message message)
    {
        _innerSync.Acknowledge(message);
    }

    public Task AcknowledgeAsync(Message message, CancellationToken cancellationToken = default)
    {
        return _innerAsync.AcknowledgeAsync(message, cancellationToken);
    }

    public void Purge()
    {
        _innerSync.Purge();
    }

    public Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        return _innerAsync.PurgeAsync(cancellationToken);
    }

    public Message[] Receive(TimeSpan? timeOut = null)
    {
        for (var i = 0; i < _maxRetries; i++)
        {
            var messages = _innerSync.Receive(timeOut);
            if (messages.Length > 0 && messages[0].Header.MessageType != MessageType.MT_NONE)
            {
                return messages;
            }
        }

        return [new Message()];
    }

    public async Task<Message[]> ReceiveAsync(TimeSpan? timeOut = null, CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < _maxRetries; i++)
        {
            var messages = await _innerAsync.ReceiveAsync(timeOut, cancellationToken);
            if (messages.Length > 0 && messages[0].Header.MessageType != MessageType.MT_NONE)
            {
                return messages;
            }
        }

        return [new Message()];
    }

    public bool Reject(Message message, MessageRejectionReason? reason = null)
    {
        return _innerSync.Reject(message, reason);
    }

    public Task<bool> RejectAsync(Message message, MessageRejectionReason? reason = null, CancellationToken cancellationToken = default)
    {
        return _innerAsync.RejectAsync(message, reason, cancellationToken);
    }

    public void Nack(Message message)
    {
        _innerSync.Nack(message);
    }

    public Task NackAsync(Message message, CancellationToken cancellationToken = default)
    {
        return _innerAsync.NackAsync(message, cancellationToken);
    }

    public bool Requeue(Message message, TimeSpan? delay = null)
    {
        return _innerSync.Requeue(message, delay);
    }

    public Task<bool> RequeueAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        return _innerAsync.RequeueAsync(message, delay, cancellationToken);
    }

    public void Dispose()
    {
        _innerSync.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _innerAsync.DisposeAsync();
    }
}
