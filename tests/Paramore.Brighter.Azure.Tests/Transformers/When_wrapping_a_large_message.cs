using Azure.Identity;
using Azure.Storage.Blobs;
using Paramore.Brighter.Azure.Tests.Helpers;
using Paramore.Brighter.Azure.Tests.TestDoubles;
using Paramore.Brighter.Transformers.Azure;
using Paramore.Brighter.Transforms.Transformers;

namespace Paramore.Brighter.Azure.Tests.Transformers;

[Trait("Category", "Azure")]
[Trait("Fragile", "CI")]
public class LargeMessagePayloadWrapTests : IDisposable
{
    private WrapPipeline<MyLargeCommand>? _transformPipeline;
    private readonly TransformPipelineBuilder _pipelineBuilder;
    private readonly Publication _publication;
    private readonly MyLargeCommand _myCommand;
    private readonly AzureBlobLuggageStore _luggageStore;
    private readonly BlobContainerClient _client;
    private string? _id;

    public LargeMessagePayloadWrapTests()
    {
        //arrange
        TransformPipelineBuilder.ClearPipelineCache();

        var mapperRegistry = new MessageMapperRegistry(
            new SimpleMessageMapperFactory(_ => new MyLargeCommandMessageMapper()),
            null);
        mapperRegistry.Register<MyLargeCommand, MyLargeCommandMessageMapper>();    
            
        _publication = new Publication{ Topic = new RoutingKey("transform.event") };
        _myCommand = new MyLargeCommand(6000);

        var bucketName = $"brightertestbucket-{Guid.NewGuid()}";
        var bucketUrl = new Uri($"{TestHelper.BlobLocation}{bucketName}");

        _client = new BlobContainerClient(bucketUrl, new AzureCliCredential());

        _luggageStore = new AzureBlobLuggageStore(new AzureBlobLuggageOptions
        {
            ContainerUri = bucketUrl,
            Credential = new AzureCliCredential()
        });
        
        var messageTransformerFactory = new SimpleMessageTransformerFactory(_ => new ClaimCheckTransformer(_luggageStore, _luggageStore));
        _pipelineBuilder = new TransformPipelineBuilder(mapperRegistry, messageTransformerFactory);
    }
    
    [Fact]
    public void When_wrapping_a_large_message()
    {
        _luggageStore.EnsureStoreExists();
        
        //act
        _transformPipeline = _pipelineBuilder.BuildWrapPipeline<MyLargeCommand>();
        var message = _transformPipeline.Wrap(_myCommand, new RequestContext(), _publication);

        //assert
        Assert.NotNull(message.Header.DataRef);
        Assert.Contains(ClaimCheckTransformer.CLAIM_CHECK, message.Header.Bag.Keys);
        Assert.Equal((string)message.Header.Bag[ClaimCheckTransformer.CLAIM_CHECK], message.Header.DataRef);
        
        _id = (string)message.Header.Bag[ClaimCheckTransformer.CLAIM_CHECK];
        Assert.Equal($"Claim Check {_id}", message.Body.Value);

        Assert.True(_luggageStore.HasClaim(_id));
    }
    
    public void Dispose()
    {
        _client.Delete();
    }
}
