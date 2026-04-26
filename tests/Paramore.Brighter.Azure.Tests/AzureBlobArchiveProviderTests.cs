using System.Text.Json;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Paramore.Brighter.Azure.Tests.TestDoubles;
using Paramore.Brighter.Storage.Azure;

namespace Paramore.Brighter.Azure.Tests;

public class AzureBlobArchiveProviderTests
{
    private AzureBlobArchiveProvider? _provider;

    private readonly SuperAwesomeCommand _command;
    private readonly SuperAwesomeEvent _event;

    private readonly JsonBodyMessageMapper<SuperAwesomeCommand> _commandMapper;
    private readonly JsonBodyMessageMapper<SuperAwesomeEvent> _eventMapper;

    private readonly Func<Message, string> _storageLocationFunction;

    public AzureBlobArchiveProviderTests()
    {
        _command = new SuperAwesomeCommand($"do the thing {Guid.NewGuid()}");
        _event = new SuperAwesomeEvent($"The thing was done {Guid.NewGuid()}");

        _commandMapper = new JsonBodyMessageMapper<SuperAwesomeCommand>();
        _eventMapper = new JsonBodyMessageMapper<SuperAwesomeEvent>();

        _storageLocationFunction = (message) => $"{message.Header.Topic}/{message.Id}".ToLower();
    }
    
    [Fact]
    public async Task GivenARequestToArchiveAMessage_TheMessageIsArchived()
    {
        var publication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeCommand"), 
            RequestType = typeof(SuperAwesomeCommand)
        };
        
        var commandMessage = _commandMapper?.MapToMessage(_command, publication);

        Assert.NotNull(commandMessage);

        var blobClient = GetClient(AccessTier.Cool).GetBlobClient(_storageLocationFunction?.Invoke(commandMessage));
        
        _provider?.ArchiveMessage(commandMessage);

        Assert.True(await blobClient.ExistsAsync());

        var tags = (await blobClient.GetTagsAsync()).Value.Tags;
        Assert.Empty(tags);

        var body = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
        
        Assert.Equal(commandMessage.Body.Value, body);

        var tier = await blobClient.GetPropertiesAsync();
        Assert.Equal(AccessTier.Cool.ToString(), tier.Value.AccessTier);
        
    }

    [Fact]
    public async Task GivenARequestToArchiveAMessage_WhenTagsAreTurnedOn_ThenTagsAreWritten()
    {
        var publication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeEvent"), 
            RequestType = typeof(SuperAwesomeEvent)
        };
        
        var eventMessage = _eventMapper.MapToMessage(_event, publication);
        
        var blobClient = GetClient(AccessTier.Hot, true).GetBlobClient(_storageLocationFunction.Invoke(eventMessage));
        
        _provider?.ArchiveMessage(eventMessage);
        
        var tier = await blobClient.GetPropertiesAsync();
        Assert.Equal(AccessTier.Hot.ToString(), tier.Value.AccessTier);
        
        var tags = (await blobClient.GetTagsAsync()).Value.Tags;

        Assert.Equal(eventMessage.Header.Topic.Value, tags["topic"]);
        Assert.Equal(eventMessage.Header.CorrelationId.Value, tags["correlationId"]);
        Assert.Equal(eventMessage.Header.MessageType.ToString(), tags["message_type"]);
        Assert.Equal(eventMessage.Header.TimeStamp.DateTime, DateTime.Parse(tags["timestamp"]));
        Assert.Equal(eventMessage.Header.ContentType!.ToString(), tags["content_type"]);
    }

    [Fact]
    public async Task GivenARequestToArchiveAMessageAsync_TheMessageIsArchived()
    {
        var publication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeCommand"),
            RequestType = typeof(SuperAwesomeCommand)
        };
        
        var commandMessage = _commandMapper.MapToMessage(_command, publication);
        
        Assert.NotNull(commandMessage);

        var blobClient = GetClient(AccessTier.Cool).GetBlobClient(_storageLocationFunction.Invoke(commandMessage));
        
        await _provider?.ArchiveMessageAsync(commandMessage, CancellationToken.None)!;

        Assert.True(await blobClient.ExistsAsync());

        var tags = (await blobClient.GetTagsAsync()).Value.Tags;
        Assert.Empty(tags);

        var body = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
        
        Assert.Equal(commandMessage.Body.Value, body);

        var tier = await blobClient.GetPropertiesAsync();
        Assert.Equal(AccessTier.Cool.ToString(), tier.Value.AccessTier);
        
    }

    [Fact]
    public async Task GivenARequestToArchiveAMessageAsync_WhenParallel_TheMessageIsArchived()
    {
        var cmdPublication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeCommand"), 
            RequestType = typeof(SuperAwesomeCommand)
        };
        
        var evtPublication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeEvent"), 
            RequestType = typeof(SuperAwesomeEvent)
        };
        
        var superAwesomeCommands = new List<SuperAwesomeCommand>();
        var superAwesomeEvents = new List<SuperAwesomeEvent>();

        var containerClient = GetClient(AccessTier.Cool);

        for (var i = 0; i < 10; i++)
        {
            superAwesomeCommands.Add(new SuperAwesomeCommand($"do the thing {Guid.NewGuid()}"));
            superAwesomeEvents.Add(new SuperAwesomeEvent($"The thing was done {Guid.NewGuid()}"));
        }

        var messages = new List<Message>();
        messages.AddRange(superAwesomeCommands.Select(c => _commandMapper.MapToMessage(c, cmdPublication)));
        messages.AddRange(superAwesomeEvents.Select(c => _eventMapper.MapToMessage(c, evtPublication)));

        await _provider?.ArchiveMessagesAsync(messages.ToArray(), CancellationToken.None)!;

        foreach (var message in messages)
        {
            var blobClient = containerClient.GetBlobClient(_storageLocationFunction.Invoke(message));
            Assert.True(await blobClient.ExistsAsync());

            var tags = (await blobClient.GetTagsAsync()).Value.Tags;
            Assert.Empty(tags);

            var body = (await blobClient.DownloadContentAsync()).Value.Content.ToString();

            string brighterBody = "";
            if (message.Header.MessageType == MessageType.MT_COMMAND)
                brighterBody = JsonSerializer.Serialize(superAwesomeCommands.First(c => c.Id == message.Id));
            else if (message.Header.MessageType == MessageType.MT_EVENT)
                brighterBody = JsonSerializer.Serialize(superAwesomeEvents.First(c => c.Id == message.Id));
            
            Assert.Equal(brighterBody, body);

            var tier = await blobClient.GetPropertiesAsync();
            Assert.Equal(AccessTier.Cool.ToString(), tier.Value.AccessTier);
        }

    }

    [Fact]
    public async Task GivenARequestToArchiveAMessageAsync_WhenTagsAreTurnedOn_ThenTagsAreWritten()
    {
        var publication = new Publication
        {
            Topic = new RoutingKey($"{Guid.NewGuid()}-SuperAwesomeEvent"), 
            RequestType = typeof(SuperAwesomeEvent)
        };
        
        var eventMessage = _eventMapper.MapToMessage(_event, publication);
        
        var blobClient = GetClient(AccessTier.Hot, true).GetBlobClient(_storageLocationFunction.Invoke(eventMessage));
        
        await _provider?.ArchiveMessageAsync(eventMessage, CancellationToken.None)!;
        
        var tier = await blobClient.GetPropertiesAsync();
        Assert.Equal(AccessTier.Hot.ToString(), tier.Value.AccessTier);
        
        var tags = (await blobClient.GetTagsAsync()).Value.Tags;

        Assert.Equal(eventMessage.Header.Topic.Value, tags["topic"]);
        Assert.Equal(eventMessage.Header.CorrelationId.Value, tags["correlationId"]);
        Assert.Equal(eventMessage.Header.MessageType.ToString(), tags["message_type"]);
        Assert.Equal(eventMessage.Header.TimeStamp.DateTime, DateTime.Parse(tags["timestamp"]));
        Assert.Equal(eventMessage.Header.ContentType!.ToString(), tags["content_type"]);
    }

    private BlobContainerClient GetClient(AccessTier tier , bool tags = false )
    {
        var options = new AzureBlobArchiveProviderOptions
        (
            new Uri("https://brighterarchivertest.blob.core.windows.net/messagearchive"), 
            new AzureCliCredential(),
            tier,
            tags
        );
        _provider = new AzureBlobArchiveProvider(options);

        return new BlobContainerClient(options.BlobContainerUri, options.TokenCredential);
    }
}
